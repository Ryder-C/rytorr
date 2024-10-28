mod client;
mod file;
mod peer;
mod swarm;
mod tracker;

use client::Client;
use once_cell::sync::Lazy;
use torrex::bencode::Torrent;

// Decode torrent file
static TORRENT: Lazy<Torrent> = Lazy::new(|| {
    Torrent::new("/home/ryder/Downloads/torrentdyne_great_rooster/torrentdyne_great_rooster_http_ipv4.probe.torrent").unwrap()
});

#[tokio::main]
async fn main() {
    // Lazy::force(&TORRENT);

    println!("{:?}", *TORRENT);

    // Build torrent client (starts p2p swarm)
    let client = Client::new(&TORRENT, 4444);
    client.start_tracking();

    loop {}
}
