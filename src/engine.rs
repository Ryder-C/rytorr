use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex, RwLock,
    },
};
use torrex::bencode::Torrent;

use crate::{
    peer::Peer,
    swarm::Swarm,
    tracker::{http, udp, Trackable, TrackerType},
};
use anyhow::{bail, Result};
use rand::{distributions, Rng};
use std::collections::HashMap;
use tracing::{debug, error, info, instrument, warn};

#[derive(Debug)]
pub enum PendingPeer {
    Outgoing(Peer),
    Incoming(Peer, TcpStream),
}

pub struct TorrentClient {
    torrent: Arc<Torrent>,
    peer_sender: Sender<PendingPeer>,
    peer_id: String,
    port: u16,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    seeders: Arc<Mutex<u32>>,
    leechers: Arc<Mutex<u32>>,
    size: u64,
}

impl TorrentClient {
    pub fn new(torrent: Torrent, port: u16) -> Self {
        let torrent = Arc::new(torrent);
        let peer_id = Self::generate_peer_id();
        let size = torrent.info.files.iter().map(|f| f.length).sum();

        let (peer_sender, peer_reciever) = mpsc::channel(50);

        let info_hash = Arc::new(torrent.info.hash.to_vec());
        let pieces = Arc::new(torrent.info.pieces.clone());
        let downloaded = Arc::new(RwLock::new(0));
        let uploaded = Arc::new(RwLock::new(0));

        Self::start_swarm(
            peer_sender.clone(),
            peer_reciever,
            peer_id.clone(),
            port,
            info_hash,
            torrent.info.name.clone(),
            torrent.info.piece_length as u64,
            pieces,
            downloaded.clone(),
            uploaded.clone(),
        );

        Self {
            torrent,
            peer_sender,
            peer_id,
            port,
            downloaded,
            uploaded,
            seeders: Arc::new(Mutex::new(0)),
            leechers: Arc::new(Mutex::new(0)),
            size,
        }
    }

    fn generate_peer_id() -> String {
        let mut peer_id = String::new();
        peer_id.push_str("-RY0000-");
        for _ in 0..12 {
            peer_id.push(rand::thread_rng().sample(distributions::Alphanumeric) as char);
        }
        peer_id
    }

    pub fn start_swarm(
        sender: Sender<PendingPeer>,
        reciever: Receiver<PendingPeer>,
        peer_id: String,
        port: u16,
        info_hash: Arc<Vec<u8>>,
        torrent_name: String,
        piece_length: u64,
        pieces: Arc<Vec<[u8; 20]>>,
        downloaded: Arc<RwLock<u64>>,
        uploaded: Arc<RwLock<u64>>,
    ) {
        tokio::spawn(async move {
            let mut swarm = Swarm::new(
                reciever,
                peer_id,
                torrent_name,
                piece_length,
                pieces.clone(),
                downloaded.clone(),
                uploaded.clone(),
            );

            tokio::spawn(Swarm::listen_for_peers(sender.clone(), port));

            swarm.start(info_hash).await;
        });
    }

    pub fn start_tracking(&self) {
        let announce_list = self.torrent.announce_list.clone();
        let info_hash = Arc::new(self.torrent.info.hash.to_vec());
        let port = self.port;
        let size = self.size;

        for url in announce_list {
            let peer_id = self.peer_id.clone();
            let downloaded = self.downloaded.clone();
            let uploaded = self.uploaded.clone();
            let seeders = self.seeders.clone();
            let leechers = self.leechers.clone();
            let sender = self.peer_sender.clone();
            let url_clone = url.clone();
            let info_hash_clone = info_hash.clone();

            tokio::spawn(async move {
                let mut tracker =
                    match Self::create_tracker(url_clone, info_hash_clone, peer_id, port, size) {
                        Ok(t) => t,
                        Err(e) => {
                            error!(url = %url, error = %e, "Failed to create tracker");
                            return;
                        }
                    };

                loop {
                    let current_downloaded = *downloaded.read().await;
                    let current_uploaded = *uploaded.read().await;
                    let left = size.saturating_sub(current_downloaded);
                    tracker.update_progress(current_downloaded, current_uploaded, left);

                    let response = match tracker.scrape() {
                        Ok(r) => r,
                        Err(e) => {
                            error!(url = %url, error = %e, "Tracker scrape error");
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            continue;
                        }
                    };

                    debug!(url = %url, ?response, "Received response from tracker");

                    for peer in response.peers {
                        if let Err(e) = sender.send(PendingPeer::Outgoing(peer)).await {
                            error!(error = %e, "Failed to send peer to swarm");
                            break;
                        }
                    }
                    if let Some(new_seeders) = response.seeders {
                        *seeders.lock().await = new_seeders;
                    }
                    if let Some(new_leechers) = response.leechers {
                        *leechers.lock().await = new_leechers;
                    }

                    tokio::time::sleep(Duration::from_secs(response.interval as u64)).await;
                }
            });
        }
    }

    fn create_tracker(
        url: String,
        info_hash: Arc<Vec<u8>>,
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Result<Box<dyn Trackable>> {
        let tracker_type = match TrackerType::type_from_url(&url) {
            Ok(typ) => typ,
            Err(_) => bail!("Unknown tracker protocol for URL: {}", url),
        };

        Ok(match tracker_type {
            TrackerType::Http => Box::new(http::Http::new(url, info_hash, peer_id, port, size)),
            TrackerType::Udp => Box::new(udp::Udp::new(url, info_hash, peer_id, port, size)?),
        })
    }
}

pub struct Engine {
    torrents: Arc<RwLock<HashMap<Vec<u8>, Arc<TorrentClient>>>>,
    port: u16,
}

impl Engine {
    pub fn new(port: u16) -> Self {
        Self {
            torrents: Arc::new(RwLock::new(HashMap::new())),
            port,
        }
    }

    #[instrument(skip(self, torrent), fields(torrent_name = %torrent.info.name, info_hash = ?torrent.info.hash))]
    pub async fn add_torrent(&self, torrent: Torrent) -> Result<()> {
        let info_hash = torrent.info.hash.to_vec();
        if self.torrents.read().await.contains_key(&info_hash) {
            warn!("Torrent already added.");
            bail!("Torrent already added.");
        }

        info!("Adding torrent");
        let client = Arc::new(TorrentClient::new(torrent, self.port));
        client.start_tracking();

        self.torrents.write().await.insert(info_hash, client);

        Ok(())
    }

    pub async fn torrent_count(&self) -> usize {
        self.torrents.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_generate_peer_id_format() {
        let id = TorrentClient::generate_peer_id();
        assert!(id.starts_with("-RY0000-"));
        assert_eq!(id.len(), 20);
        assert!(id[8..].chars().all(char::is_alphanumeric));
    }

    #[test]
    fn test_create_tracker_invalid() {
        let info_hash = Arc::new(vec![0u8; 20]);
        let peer_id = TorrentClient::generate_peer_id();
        let result =
            TorrentClient::create_tracker("invalid-url".to_string(), info_hash, peer_id, 6881, 100);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("Unknown tracker protocol"));
    }
}
