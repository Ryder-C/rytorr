mod engine;
mod file;
mod peer;
mod swarm;
mod tracker;

use engine::Engine;
use std::env;
use torrex::bencode::Torrent;

#[tokio::main]
async fn main() {
    // Get torrent file path from args or use default
    let args: Vec<String> = env::args().collect();
    let torrent_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "test_torrents/ubuntu.torrent".to_string());

    println!("Loading torrent from: {}", torrent_path);
    // Load torrent at runtime
    let torrent = match Torrent::new(&torrent_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to load torrent file '{}': {}", torrent_path, e);
            return;
        }
    };

    println!("Starting {:?}", &torrent.info.name);

    // Build torrent client (starts p2p swarm)
    let client = Engine::new(torrent, 4444);
    client.start_tracking();

    // Keep main thread alive (temporary)
    loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
