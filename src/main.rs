mod client;
mod file;
mod peer;
mod swarm;
mod tracker;

use client::Client;
use once_cell::sync::Lazy;
use torrex::bencode::Torrent;

// Decode torrent file
static TORRENT: Lazy<Torrent> = Lazy::new(|| Torrent::new("test_torrents/ubuntu.torrent").unwrap());

#[tokio::main]
async fn main() {
    println!("{:?}", TORRENT);

    // Build torrent client
    let client = Client::new(&TORRENT, 4444);
    client.start_tracking();
}
