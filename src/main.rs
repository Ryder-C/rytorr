mod bencode;
mod client;
mod file;
mod peer;
mod swarm;
mod tracker;

use bencode::Torrent;
use client::Client;
use once_cell::sync::Lazy;

// Decode torrent file
static TORRENT: Lazy<Torrent> = Lazy::new(|| {
    Torrent::new("~/Downloads/Kali.torrent").unwrap()
});

#[tokio::main]
async fn main() {
    println!("{:?}", TORRENT);

    // Build torrent client
    let client = Client::new(&TORRENT, 4444);
    client.start_tracking();
}
