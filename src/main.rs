mod bencode;
mod client;
mod swarm;
mod tracker;

use bencode::Torrent;
use client::Client;

#[tokio::main]
async fn main() {
    // Decode torrent file
    let torrent = Torrent::new("../Kali.torrent").unwrap();
    println!("{:?}", torrent);
    // Build torrent client
    let client = Client::new(torrent, 4444);
    client.start_tracking();
}
