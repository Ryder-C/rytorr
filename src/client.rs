use std::{sync::Arc, thread, time::Duration};
use tokio::sync::RwLock;

use crate::{
    bencode::Torrent,
    tracker::{http, udp, Trackable, TrackerType},
};
use anyhow::{bail, Result};
use rand::{distributions, Rng};

pub struct Client {
    torrent: Torrent,
    peer_id: String,
    port: u16,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    size: u64,
}

impl Client {
    pub fn new(torrent: Torrent, port: u16) -> Self {
        let peer_id = Self::generate_peer_id();
        let size = torrent.info.files.iter().map(|f| f.length).sum();
        Self {
            torrent,
            peer_id,
            port,
            downloaded: Arc::new(RwLock::new(0)),
            uploaded: Arc::new(RwLock::new(0)),
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

    pub fn start_tracking(&self) {
        let announce_list = self.torrent.announce_list.clone();
        let info_hash = self.torrent.info.hash;
        let port = self.port;
        let size = self.size;

        for url in announce_list {
            let peer_id = self.peer_id.clone();
            let downloaded = self.downloaded.clone();
            let uploaded = self.uploaded.clone();

            tokio::spawn(async move {
                let mut tracker =
                    Self::create_tracker(url, info_hash, peer_id, port, size).unwrap();

                loop {
                    tracker.update_progress(*downloaded.read().await, *uploaded.read().await);
                    let response = tracker.scrape().unwrap();

                    println!("Recieved response: {:?}", response);

                    // Update seeders, leechers, and peers
                    // todo!();

                    thread::sleep(Duration::from_secs(response.interval as u64));
                }
            });
        }
    }

    fn create_tracker(
        url: String,
        info_hash: [u8; 20],
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Result<Box<dyn Trackable>> {
        let tracker_type = match TrackerType::type_from_url(&url) {
            Ok(typ) => typ,
            Err(_) => bail!("Unknown tracker protocol"),
        };

        Ok(match tracker_type {
            TrackerType::Http => Box::new(http::Http::new(url, &info_hash, peer_id, port, size)),
            TrackerType::Udp => Box::new(udp::Udp::new(url, info_hash, peer_id, port, size)?),
        })
    }
}
