use crossterm::{
    cursor,
    style::{self, Stylize}, // Add Stylize for potential coloring
    terminal::{self, ClearType},
    QueueableCommand, // Use queue for efficiency
};
use std::{
    collections::HashMap,
    io::{stdout, Write}, // Use stdout directly
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock}; // Added RwLock
use tokio::time::Instant; // Added Instant
use tracing::{error, trace, warn}; // Added warn

use crate::{peer::Peer, swarm::handlers::SwarmCommandHandler}; // Adjust path as needed

// Type alias for clarity
type PeerCmdSender = UnboundedSender<Box<dyn SwarmCommandHandler + Send>>;
type PeerMap = Arc<Mutex<HashMap<Peer, PeerCmdSender>>>;

/// Task to periodically update a status line at the bottom of the terminal.
#[tracing::instrument(skip(
    torrent_name,
    downloaded,
    total_size,
    next_tracker_announce_times,
    peer_cmd_senders
))]
pub async fn run_status_loop(
    torrent_name: String,
    downloaded: Arc<RwLock<u64>>,
    total_size: u64,
    next_tracker_announce_times: Arc<Mutex<HashMap<String, Instant>>>,
    peer_cmd_senders: PeerMap, // Keep this for peer count, or decide if it's needed elsewhere
) {
    let mut stdout = stdout(); // Get handle outside loop
    trace!(%torrent_name, "Starting status update loop.");

    loop {
        // --- Get Data ---
        let peer_count = peer_cmd_senders.lock().await.len();
        let downloaded_bytes = *downloaded.read().await;
        let progress = if total_size > 0 {
            (downloaded_bytes as f64 / total_size as f64) * 100.0
        } else {
            0.0
        };

        let mut time_to_next_announce_secs = "N/A".to_string();
        let now = Instant::now();
        let tracker_times = next_tracker_announce_times.lock().await;
        if let Some(min_instant) = tracker_times.values().min() {
            if *min_instant > now {
                time_to_next_announce_secs =
                    format!("{}s", min_instant.duration_since(now).as_secs());
            } else {
                time_to_next_announce_secs = "now".to_string(); // Or some other indicator if it's past
            }
        } else if !tracker_times.is_empty() {
            // This case might occur if all stored instants are in the past.
            // Or if there's a brief moment during initialization or shutdown.
            warn!(%torrent_name, "No future tracker announce times found, displaying 'now/N/A'");
            time_to_next_announce_secs = "...".to_string();
        }
        drop(tracker_times); // Release lock ASAP

        trace!(%torrent_name, peer_count, progress, %time_to_next_announce_secs, "Fetched data for status update.");

        // --- Render Status ---
        match terminal::size() {
            Ok((_width, height)) => {
                let status_text = format!(
                    "Torrent: {} | Peers: {} | Progress: {:.2}% | Next announce: {}",
                    torrent_name, peer_count, progress, time_to_next_announce_secs
                );

                let render_result = stdout
                    .queue(cursor::SavePosition)
                    // Move to last line, handle potential terminal height of 0
                    .and_then(|s| s.queue(cursor::MoveTo(0, height.saturating_sub(1))))
                    .and_then(|s| s.queue(terminal::Clear(ClearType::CurrentLine)))
                    .and_then(|s| {
                        // Use StyledContent for potential future styling
                        s.queue(style::PrintStyledContent(status_text.cyan()))
                    })
                    .and_then(|s| s.queue(cursor::RestorePosition))
                    .and_then(|s| s.flush()); // Flush all queued commands

                if let Err(e) = render_result {
                    // Log error but don't crash the loop, terminal might be unavailable
                    error!(error = %e, "Failed to update status line for {}", torrent_name);
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to get terminal size for {}", torrent_name);
            }
        }

        // --- Wait ---
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Note: Loop runs indefinitely. Add shutdown mechanism if needed.
    // warn!("Status update loop finished unexpectedly for {}", torrent_name); // Log if loop exits
}
