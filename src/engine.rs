use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        watch, Mutex, RwLock,
    },
    time::Instant,
};
use torrex::bencode::Torrent;

use crate::{
    peer::Peer,
    status,
    swarm::{PeerMapArc, Swarm},
    tracker::{http, udp, Trackable, TrackerType},
};
use anyhow::{bail, Result};
use rand::{distributions, Rng};
use std::collections::HashMap;
use tracing::{debug, error, info, instrument, trace, warn};

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
    peer_cmd_senders: PeerMapArc,
    next_tracker_announce_times: Arc<Mutex<HashMap<String, Instant>>>,
}

impl TorrentClient {
    pub fn new(torrent: Torrent, port: u16) -> Self {
        let torrent_arc = Arc::new(torrent);
        let peer_id = Self::generate_peer_id();
        let size = torrent_arc.info.files.iter().map(|f| f.length).sum();

        let (peer_sender, peer_reciever) = mpsc::channel(50);

        let info_hash = Arc::new(torrent_arc.info.hash.to_vec());
        let pieces = Arc::new(torrent_arc.info.pieces.clone());
        let downloaded = Arc::new(RwLock::new(0));
        let uploaded = Arc::new(RwLock::new(0));
        let next_tracker_announce_times = Arc::new(Mutex::new(HashMap::new()));

        let peer_cmd_senders: PeerMapArc = Arc::new(Mutex::new(HashMap::new()));

        Self::start_swarm(
            peer_sender.clone(),
            peer_reciever,
            peer_id.clone(),
            port,
            info_hash,
            torrent_arc.info.name.clone(),
            torrent_arc.info.piece_length as u64,
            pieces,
            downloaded.clone(),
            uploaded.clone(),
            peer_cmd_senders.clone(),
            size,
            next_tracker_announce_times.clone(),
        );

        Self {
            torrent: torrent_arc,
            peer_sender,
            peer_id,
            port,
            downloaded,
            uploaded,
            seeders: Arc::new(Mutex::new(0)),
            leechers: Arc::new(Mutex::new(0)),
            size,
            peer_cmd_senders,
            next_tracker_announce_times,
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
        peer_cmd_senders: PeerMapArc,
        total_size: u64,
        next_tracker_announce_times: Arc<Mutex<HashMap<String, Instant>>>,
    ) {
        tokio::spawn(async move {
            let mut swarm = Swarm::new(
                reciever,
                peer_id,
                torrent_name.clone(),
                piece_length,
                pieces.clone(),
                downloaded.clone(),
                uploaded.clone(),
                peer_cmd_senders.clone(),
            );

            let status_torrent_name = torrent_name;
            let status_downloaded = downloaded;
            let status_total_size = total_size;
            let status_peer_map = peer_cmd_senders;
            let status_tracker_times = next_tracker_announce_times;

            tokio::spawn(status::run_status_loop(
                status_torrent_name,
                status_downloaded,
                status_total_size,
                status_tracker_times,
                status_peer_map,
            ));
            info!("Status loop spawned for swarm.");

            tokio::spawn(Swarm::listen_for_peers(sender.clone(), port));

            swarm.start(info_hash).await;
            warn!("Swarm processing loop finished.");
        });
    }

    pub fn start_tracking(&self) {
        const LOW_PEER_THRESHOLD: usize = 5;
        const MIN_ANNOUNCE_INTERVAL_SECS: u64 = 30;

        let announce_list = self.torrent.announce_list.clone();
        let info_hash_arc = Arc::new(self.torrent.info.hash.to_vec());
        let port = self.port;
        let size = self.size;
        let next_tracker_announce_times_clone = self.next_tracker_announce_times.clone();

        let (low_peer_tx, low_peer_rx) = watch::channel(false);

        let monitor_peer_map = self.peer_cmd_senders.clone();
        let monitor_torrent_name = self.torrent.info.name.clone();
        let monitor_low_peer_tx = low_peer_tx.clone();

        tokio::spawn(async move {
            let mut currently_low = false;
            if monitor_low_peer_tx.is_closed() {
                debug!(torrent_name = %monitor_torrent_name, "Low peer notifier channel closed at monitor start. Task exiting.");
                return;
            }
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;

                let num_peers = monitor_peer_map.lock().await.len();
                let is_low = num_peers < LOW_PEER_THRESHOLD;

                if is_low != currently_low {
                    debug!(torrent_name = %monitor_torrent_name, num_peers, threshold = LOW_PEER_THRESHOLD, new_state_is_low = is_low, "Peer count threshold state changed.");
                    if monitor_low_peer_tx.send(is_low).is_err() {
                        warn!(torrent_name = %monitor_torrent_name, "Failed to send low peer notification; receivers likely dropped. Monitor task exiting.");
                        break;
                    }
                    currently_low = is_low;
                }

                if monitor_low_peer_tx.is_closed() {
                    warn!(torrent_name = %monitor_torrent_name, "Low peer notifier channel (sender side) closed. Monitor task exiting.");
                    break;
                }
            }
        });

        for tracker_url_from_list in announce_list {
            let peer_id = self.peer_id.clone();
            let downloaded_arc = self.downloaded.clone();
            let uploaded_arc = self.uploaded.clone();
            let seeders_arc = self.seeders.clone();
            let leechers_arc = self.leechers.clone();
            let sender_clone = self.peer_sender.clone();

            let url_for_tracker_creation = tracker_url_from_list.clone();
            let key_url_for_task = tracker_url_from_list.clone();

            let info_hash_clone_tracker = info_hash_arc.clone();
            let tracker_times_updater_clone = next_tracker_announce_times_clone.clone();
            let mut low_peer_rx_clone = low_peer_rx.clone();

            tokio::spawn(async move {
                let mut tracker = match Self::create_tracker(
                    url_for_tracker_creation,
                    info_hash_clone_tracker,
                    peer_id,
                    port,
                    size,
                ) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(url = %key_url_for_task, error = %e, "Failed to create tracker");
                        return;
                    }
                };

                loop {
                    let current_downloaded = *downloaded_arc.read().await;
                    let current_uploaded = *uploaded_arc.read().await;
                    let left = size.saturating_sub(current_downloaded);
                    tracker.update_progress(current_downloaded, current_uploaded, left);

                    let response = match tracker.scrape() {
                        Ok(r) => r,
                        Err(e) => {
                            error!(url = %key_url_for_task, error = %e, "Tracker scrape error");
                            let error_sleep_duration =
                                Duration::from_secs(MIN_ANNOUNCE_INTERVAL_SECS);
                            {
                                let mut times_map = tracker_times_updater_clone.lock().await;
                                times_map.insert(
                                    key_url_for_task.clone(),
                                    Instant::now() + error_sleep_duration,
                                );
                            }
                            tokio::time::sleep(error_sleep_duration).await;
                            continue;
                        }
                    };

                    debug!(url = %key_url_for_task, peers_count = response.peers.len(), interval = response.interval, "Received response from tracker");

                    for peer_info in response.peers {
                        if let Err(e) = sender_clone.send(PendingPeer::Outgoing(peer_info)).await {
                            error!(error = %e, "Failed to send peer to swarm");
                        }
                    }
                    if let Some(new_seeders) = response.seeders {
                        *seeders_arc.lock().await = new_seeders;
                    }
                    if let Some(new_leechers) = response.leechers {
                        *leechers_arc.lock().await = new_leechers;
                    }

                    let base_announce_interval_secs =
                        std::cmp::max(response.interval as u64, MIN_ANNOUNCE_INTERVAL_SECS);
                    let base_sleep_duration = Duration::from_secs(base_announce_interval_secs);

                    {
                        let mut times_map = tracker_times_updater_clone.lock().await;
                        times_map.insert(
                            key_url_for_task.clone(),
                            Instant::now() + base_sleep_duration,
                        );
                    }

                    trace!(url = %key_url_for_task, interval = ?base_sleep_duration, "Tracker loop: planning to sleep or wait for low peer notification.");

                    tokio::select! {
                        biased;

                        Ok(()) = low_peer_rx_clone.changed() => {
                            if *low_peer_rx_clone.borrow() {
                                info!(url = %key_url_for_task, "Low peer count signal received. Announcing sooner.");
                            } else {
                                trace!(url = %key_url_for_task, "Peer count no longer low (notification received). Proceeding with announce.");
                            }
                        }
                        _ = tokio::time::sleep(base_sleep_duration) => {
                            trace!(url = %key_url_for_task, "Tracker sleep normally elapsed.");
                        }
                    }
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

        self.torrents
            .write()
            .await
            .insert(info_hash, client.clone());

        client.start_tracking();

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
