use crossterm::{
    cursor,
    style::{self, Stylize}, // Add Stylize for potential coloring
    terminal::{self, ClearType},
    QueueableCommand, // Use queue for efficiency
};
use std::{
    collections::HashMap,
    io::{stdout, Stdout, Write}, // Use stdout directly
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tracing::{error, trace}; // Use tracing for errors

use crate::{peer::Peer, swarm::handlers::SwarmCommandHandler}; // Adjust path as needed

// Type alias for clarity
type PeerCmdSender = UnboundedSender<Box<dyn SwarmCommandHandler + Send>>;
type PeerMap = Arc<Mutex<HashMap<Peer, PeerCmdSender>>>;

/// Task to periodically update a status line at the bottom of the terminal.
#[tracing::instrument(skip(peer_cmd_senders))]
pub async fn run_status_loop(peer_cmd_senders: PeerMap) {
    let mut stdout = stdout(); // Get handle outside loop
    trace!("Starting status update loop.");

    loop {
        // --- Get Data ---
        let count = peer_cmd_senders.lock().await.len();
        trace!(peer_count = count, "Fetched peer count for status update.");

        // --- Render Status ---
        match terminal::size() {
            Ok((_width, height)) => {
                // Queue commands for batch execution
                let render_result = stdout
                    .queue(cursor::SavePosition)
                    // Move to last line, handle potential terminal height of 0
                    .and_then(|s| s.queue(cursor::MoveTo(0, height.saturating_sub(1))))
                    .and_then(|s| s.queue(terminal::Clear(ClearType::CurrentLine)))
                    .and_then(|s| {
                        // Use StyledContent for potential future styling
                        s.queue(style::PrintStyledContent(
                            format!("Connected Peers: {}", count).cyan(), // Example styling
                        ))
                    })
                    .and_then(|s| s.queue(cursor::RestorePosition))
                    .and_then(|s| s.flush()); // Flush all queued commands

                if let Err(e) = render_result {
                    // Log error but don't crash the loop, terminal might be unavailable
                    error!(error = %e, "Failed to update status line");
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to get terminal size");
            }
        }

        // --- Wait ---
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // Note: Loop runs indefinitely. Add shutdown mechanism if needed.
    // warn!("Status update loop finished unexpectedly."); // Log if loop exits
}
