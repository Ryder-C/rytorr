mod engine;
mod file;
mod peer;
mod swarm;
mod tracker;

use engine::Engine;
use std::{fs, process};
use torrex::bencode::Torrent;

const TORRENT_DIR: &str = "test_torrents";

#[tokio::main]
async fn main() {
    println!("Starting Rytorr Engine...");

    // --- Load Torrents ---
    let torrent_files = match fs::read_dir(TORRENT_DIR) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Error reading torrent directory '{}': {}", TORRENT_DIR, e);
            process::exit(1);
        }
    };

    let mut torrents = Vec::new();
    for entry in torrent_files {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Error reading directory entry: {}", e);
                continue;
            }
        };
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "torrent") {
            println!("Loading torrent file: {:?}", path);
            match Torrent::new(path.to_str().unwrap()) {
                // Assuming valid UTF-8 paths
                Ok(torrent) => {
                    println!("Successfully decoded: {}", torrent.info.name);
                    torrents.push(torrent);
                }
                Err(e) => {
                    eprintln!("Failed to decode torrent file {:?}: {}", path, e);
                }
            }
        }
    }

    if torrents.is_empty() {
        println!(
            "No valid torrent files found in '{}'. Exiting.",
            TORRENT_DIR
        );
        process::exit(0);
    }

    // --- Initialize Engine ---
    let engine = Engine::new(4444); // Port can be configurable

    // --- Add Torrents to Engine ---
    for torrent in torrents {
        let torrent_name = torrent.info.name.clone(); // Clone name for error message
        if let Err(e) = engine.add_torrent(torrent).await {
            eprintln!("Failed to add torrent '{}': {}", torrent_name, e);
        }
    }

    println!(
        "Engine running with {} torrents. Press Ctrl+C to exit.",
        engine.torrent_count().await
    );
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");

    println!("Shutting down...");

    // TODO: Implement graceful shutdown for the engine and swarms
}
