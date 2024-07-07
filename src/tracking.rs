use crate::{constants::PORT, peer::Peer, torrent_parsing::Torrent};
use anyhow::{bail, Result};
use async_trait::async_trait;
use rand::{distributions, Rng};

pub mod http_tracker;
pub mod udp_tracker;

#[async_trait]
pub trait Trackable: Send + Sync {
    async fn announce(&mut self) -> Result<(u32, Vec<Peer>)>;
    async fn request(&self) -> Result<(u32, Vec<Peer>)>;

    fn get_info_hash(&self) -> [u8; 20];
    fn get_peer_id(&self) -> String;

    fn set_uploaded(&mut self, uploaded: u64);
    fn set_downloaded(&mut self, downloaded: u64);
    fn set_event(&mut self, event: String);
}

pub async fn get_tracker(torrent: Torrent) -> Result<(Box<dyn Trackable>, Vec<Peer>, u32, String)> {
    let mut peer_id = String::new();
    peer_id.push_str("-RY0000-");
    for _ in 0..12 {
        peer_id.push(rand::thread_rng().sample(distributions::Alphanumeric) as char);
    }

    if let Some(announce_list) = torrent.announce_list {
        let announce_list: Vec<String> = announce_list.into_iter().flatten().collect();
        for url in announce_list {
            let peer_id = peer_id.clone();
            let mut tracker: Box<dyn Trackable> = if url.starts_with("http") {
                Box::new(http_tracker::HTTP::new(
                    url,
                    torrent.info_hash,
                    peer_id.clone(),
                    PORT,
                    0,
                    0,
                    0,
                    "started".to_owned(),
                ))
            } else if url.starts_with("udp") {
                Box::new(udp_tracker::UDP::new(
                    url,
                    torrent.info_hash,
                    peer_id.clone(),
                    PORT,
                    0,
                    0,
                    0,
                    "started".to_owned(),
                ))
            } else {
                panic!("Unrecognized announce protocol")
            };

            match tracker.announce().await {
                Ok((interval, peers)) => return Ok((tracker, peers, interval, peer_id)),
                Err(e) => bail!(e),
            }
        }
    } else if let Some(announce) = torrent.announce {
        let mut tracker: Box<dyn Trackable> = if announce.starts_with("http") {
            Box::new(http_tracker::HTTP::new(
                announce,
                torrent.info_hash,
                peer_id.clone(),
                PORT,
                0,
                0,
                0,
                "started".to_owned(),
            ))
        } else if announce.starts_with("udp") {
            Box::new(udp_tracker::UDP::new(
                announce,
                torrent.info_hash,
                peer_id.clone(),
                PORT,
                0,
                0,
                0,
                "started".to_owned(),
            ))
        } else {
            panic!("Unrecognized announce protocol")
        };

        match tracker.announce().await {
            Ok((interval, peers)) => return Ok((tracker, peers, interval, peer_id)),
            Err(e) => return Err(e),
        }
    } else {
        unimplemented!("DHT is not implemented yet")
    }

    bail!("Failed to announce tracker");
}
