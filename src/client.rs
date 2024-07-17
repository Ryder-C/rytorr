use std::{sync::Arc, thread, time::Duration};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex, RwLock,
};

use crate::{
    bencode::Torrent,
    peer::Peer,
    swarm::Swarm,
    tracker::{http, udp, Trackable, TrackerType},
};
use anyhow::{bail, Result};
use rand::{distributions, Rng};

pub struct Client {
    torrent: &'static Torrent,
    peer_sender: Sender<Vec<Peer>>,
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
        let (peer_sender, peer_reciever) = mpsc::channel(torrent.announce_list.len());
        Self::start_swarm(peer_reciever);

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

    pub fn start_swarm(reciever: Receiver<Vec<Peer>>) {
        tokio::spawn(async move {
            let mut swarm = Swarm::new(reciever);
            swarm.start().await;
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
                    sender.send(response.peers).await.unwrap();
                    if let Some(new_seeders) = response.seeders {
                        *seeders.lock().await = new_seeders;
                    }
                    if let Some(new_leechers) = response.leechers {
                        *leechers.lock().await = new_leechers;
                    }

                    thread::sleep(Duration::from_secs(response.interval as u64));
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
