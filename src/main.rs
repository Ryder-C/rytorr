mod engine;
mod file;
mod peer;
mod swarm;
mod tracker;

use engine::Engine;
use futures::future::join_all;
use std::{fs, process};
use torrex::bencode::Torrent;
use tracing::{error, info, warn};

const TORRENT_DIR: &str = "test_torrents";

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    info!("Starting Rytorr Engine...");

    // --- Load Torrents ---
    let torrent_files = match fs::read_dir(TORRENT_DIR) {
        Ok(entries) => entries,
        Err(e) => {
            error!(directory = TORRENT_DIR, error = %e, "Error reading torrent directory");
            process::exit(1);
        }
    };

    let mut torrents = vec![];
    for entry in torrent_files {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Error reading directory entry");
                continue;
            }
        };
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "torrent") {
            info!(file_path = ?path, "Loading torrent file");
            match Torrent::new(path.to_str().unwrap()) {
                // Assuming valid UTF-8 paths
                Ok(torrent) => {
                    info!(name = %torrent.info.name, "Successfully decoded torrent");
                    torrents.push(torrent);
                }
                Err(e) => {
                    error!(file_path = ?path, error = %e, "Failed to decode torrent file");
                }
            }
        }
    }

    if torrents.is_empty() {
        warn!(
            directory = TORRENT_DIR,
            "No valid torrent files found. Exiting."
        );
        process::exit(0);
    }

    // --- Initialize Engine ---
    // TODO: Make port configurable via args/config
    let port = 4444;
    info!(port = port, "Initializing engine");
    let engine = Engine::new(port);

    // --- Add Torrents to Engine ---
    let mut add_futures = Vec::new();
    for torrent in torrents {
        // Add torrents concurrently
        add_futures.push(engine.add_torrent(torrent));
    }
    // Wait for all add operations to complete
    let results = join_all(add_futures).await;
    for result in results {
        if let Err(e) = result {
            // We need the original torrent list to get the name here, or pass it differently
            // For now, just log the error generically
            error!(error = %e, "Failed to add torrent");
        }
    }

    let count = engine.torrent_count().await;
    info!(count = count, "Engine running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");

    info!("Shutting down...");

    // TODO: Implement graceful shutdown for the engine and swarms
}
