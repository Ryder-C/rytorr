mod constants;
mod message;
mod peer;
mod scheduler;
mod swarm;
mod torrent_parsing;
mod tracking;

use bendy::decoding::FromBencode;
use scheduler::Scheduler;
use std::fs;
use tokio::time;
use torrent_parsing::Torrent;

#[tokio::main]
async fn main() {
    let object = fs::read("example_torrents/debian.iso.torrent").unwrap();
    let torrent = Torrent::from_bencode(&object).unwrap();

    // Announce to tracker
    let mut scheduler = Scheduler::new(torrent).await;

    // Schedule occasional requests
    scheduler.schedule_requests().await;

    time::sleep(time::Duration::from_secs(1000)).await;
}
