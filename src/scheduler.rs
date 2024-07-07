use std::sync::Arc;

use tokio::sync::Mutex;

use tokio::time;

use crate::{
    swarm::Swarm,
    torrent_parsing::Torrent,
    tracking::{self, Trackable},
};

pub struct Scheduler {
    pub interval: Arc<Mutex<u32>>,
    pub swarm: Arc<Mutex<Swarm>>,

    tracker: Arc<Mutex<Box<dyn Trackable>>>,
}

impl Scheduler {
    pub async fn new(torrent: Torrent) -> Self {
        let info_hash = torrent.info_hash;
        let (tracker, peers, interval, peer_id) = tracking::get_tracker(torrent).await.unwrap();
        let swarm = Swarm::new(peers, info_hash.to_vec(), peer_id);
        Self {
            interval: Arc::new(Mutex::new(interval)),
            swarm: Arc::new(Mutex::new(swarm)),
            tracker: Arc::new(Mutex::new(tracker)),
        }
    }

    pub async fn schedule_requests(&mut self) {
        let interval = Arc::clone(&self.interval);
        let tracker = Arc::clone(&self.tracker);
        let swarm = Arc::clone(&self.swarm);

        tokio::spawn(async move {
            loop {
                let wait_secs = *interval.lock().await;
                println!("Waiting {} seconds", wait_secs);
                time::sleep(time::Duration::from_secs(wait_secs as u64)).await;

                let (new_interval, new_peers) = {
                    let mut tracker = tracker.lock().await;
                    tracker.set_event("".to_owned());
                    tracker.request().await.unwrap()
                };

                *interval.lock().await = new_interval;
                swarm.lock().await.update_peers(new_peers);
            }
        });

        self.swarm.lock().await.spawn_connections();
    }
}
