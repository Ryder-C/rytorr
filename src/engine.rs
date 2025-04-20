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

#[derive(Debug)]
pub enum PendingPeer {
    Outgoing(Peer),
    Incoming(Peer, TcpStream),
}

pub struct Engine {
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

impl Engine {
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
            size,
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
        size: u64,
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
                size,
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
                let mut tracker = match Self::create_tracker(url_clone, info_hash_clone, peer_id, port, size) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Failed to create tracker for {}: {}", url, e);
                        return;
                    }
                };

                loop {
                    tracker.update_progress(*downloaded.read().await, *uploaded.read().await);
                    let response = match tracker.scrape() {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("Tracker scrape error for {}: {}", url, e);
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            continue;
                        }
                    };

                    println!("Received response from {}: {:?}", url, response);

                    for peer in response.peers {
                        if let Err(e) = sender.send(PendingPeer::Outgoing(peer)).await {
                            eprintln!("Failed to send peer to swarm: {}", e);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_generate_peer_id_format() {
        let id = Engine::generate_peer_id();
        assert!(id.starts_with("-RY0000-"));
        assert_eq!(id.len(), 20);
    }

    #[test]
    fn test_create_tracker_invalid() {
        let dummy_hash = Arc::new(vec![0u8; 20]);
        let result = Engine::create_tracker(
            "ftp://example.com".to_string(),
            dummy_hash,
            "peerid".to_string(),
            6881,
            100,
        );
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("Unknown tracker protocol"));
    }
}
