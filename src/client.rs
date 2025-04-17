use std::{sync::Arc, thread, time::Duration};
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

pub struct Client {
    torrent: &'static Torrent,
    peer_sender: Sender<PendingPeer>,
    peer_id: String,
    port: u16,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    seeders: Arc<Mutex<u32>>,
    leechers: Arc<Mutex<u32>>,
    size: u64,
}

impl Client {
    pub fn new(torrent: &'static Torrent, port: u16) -> Self {
        let peer_id = Self::generate_peer_id();
        let size = torrent.info.files.iter().map(|f| f.length).sum();

        // Peer communication channel buffer size of number of trackers
        let (peer_sender, peer_reciever) = mpsc::channel(50);
        Self::start_swarm(
            peer_sender.clone(),
            peer_reciever,
            peer_id.clone(),
            &torrent.info.hash,
            port,
        );

        Self {
            torrent,
            peer_sender,
            peer_id,
            port,
            downloaded: Arc::new(RwLock::new(0)),
            uploaded: Arc::new(RwLock::new(0)),
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
        info_hash: &'static [u8],
        port: u16,
    ) {
        tokio::spawn(async move {
            let mut swarm = Swarm::new(reciever, peer_id);

            tokio::spawn(Swarm::listen_for_peers(sender, port));

            swarm.start(info_hash).await; // Loops indefinitely
        });
    }

    pub fn start_tracking(&self) {
        let announce_list = self.torrent.announce_list.clone();
        let info_hash = &self.torrent.info.hash;
        let port = self.port;
        let size = self.size;

        for url in announce_list {
            let peer_id = self.peer_id.clone();
            let downloaded = self.downloaded.clone();
            let uploaded = self.uploaded.clone();
            let seeders = self.seeders.clone();
            let leechers = self.leechers.clone();
            let sender = self.peer_sender.clone();

            tokio::spawn(async move {
                let mut tracker =
                    Self::create_tracker(url, info_hash, peer_id, port, size).unwrap();

                loop {
                    tracker.update_progress(*downloaded.read().await, *uploaded.read().await);
                    let response = tracker.scrape().unwrap();

                    println!("Recieved response: {:?}", response);

                    // Update seeders, leechers, and peers
                    for peer in response.peers {
                        println!("Sending peer: {:?}", peer);
                        sender.send(PendingPeer::Outgoing(peer)).await.unwrap();
                        println!("Sent peer");
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
        info_hash: &'static [u8],
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Result<Box<dyn Trackable>> {
        let tracker_type = match TrackerType::type_from_url(&url) {
            Ok(typ) => typ,
            Err(_) => bail!("Unknown tracker protocol"),
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
    
    #[test]
    fn test_generate_peer_id_format() {
        let id = Client::generate_peer_id();
        // should start with prefix and be 20 chars
        assert!(id.starts_with("-RY0000-"));
        assert_eq!(id.len(), 20);
    }

    #[test]
    fn test_create_tracker_invalid() {
        let result = Client::create_tracker("ftp://example.com".to_string(), b"hashhashhashhashhash" as &[u8], "peerid".to_string(), 6881, 100);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("Unknown tracker protocol"));
    }
}
