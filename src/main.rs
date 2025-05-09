mod engine;
mod file;
mod peer;
mod status;
mod swarm;
mod tracker;

use engine::Engine;
use std::{env, process};
use torrex::bencode::Torrent;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber to write to stderr
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO) // Keep level INFO for now
        .with_writer(std::io::stderr) // Send logs to stderr
        .init();

    info!("Starting Rytorr Engine...");

    // --- Get Torrent File from Argument ---
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        error!("Usage: rytorr <torrent_file_path>");
        process::exit(1);
    }
    let torrent_file_path = &args[1];
    info!(file_path = torrent_file_path, "Loading torrent file");

    // --- Load Torrent ---
    let torrent = match Torrent::new(torrent_file_path) {
        Ok(t) => {
            info!(name = %t.info.name, "Successfully decoded torrent");
            t
        }
        Err(e) => {
            error!(file_path = torrent_file_path, error = %e, "Failed to decode torrent file");
            process::exit(1);
        }
    };

    // --- Initialize Engine ---
    let port = 4444;
    info!(port = port, "Initializing engine");
    let engine = Engine::new(port);

    engine.add_torrent(torrent).await.unwrap();

    info!("Engine running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");

    info!("Shutting down...");

    // TODO: Implement graceful shutdown for the engine and swarms
}
