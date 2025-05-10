use std::collections::HashSet;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing::{debug, error, info, instrument, trace};

use crate::peer::Peer;
use crate::swarm::handlers::{self, ChokePeerCommand, UnchokePeerCommand}; // For command handlers
use crate::swarm::PeerMapArc;

/// Periodically reviews connected peers and decides which ones to choke/unchoke.
#[instrument(skip_all)]
pub(crate) async fn choking_loop(senders: PeerMapArc, upload_slots: usize) {
    info!("Starting choking loop");
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await; // Consume the initial immediate tick

    loop {
        interval.tick().await;
        debug!("Evaluating peers for choking/unchoking");

        let senders_map = senders.lock().await;
        let peers: Vec<Peer> = senders_map.keys().cloned().collect();
        let peer_count = peers.len();
        trace!(peer_count, "Found peers for choke evaluation");

        if peers.is_empty() {
            trace!("No peers to evaluate, skipping choke cycle.");
            drop(senders_map); // Release lock before continuing
            continue;
        }

        let mut unchoke_candidates = HashSet::new();

        if peer_count <= upload_slots {
            // Unchoke everyone if we have few peers
            debug!(
                peer_count,
                max_slots = upload_slots,
                "Unchoking all peers (below slot limit)"
            );
            unchoke_candidates.extend(peers.iter().cloned());
        } else {
            // --- Select top peers (Replace with actual rate logic later) ---
            // For now, just pick the first few as a placeholder
            let mut main_unchoke = peers
                .iter()
                .take(upload_slots)
                .cloned()
                .collect::<HashSet<_>>();
            debug!(
                peer_count = main_unchoke.len(),
                "Selected main unchoke candidates (placeholder logic)"
            );
            unchoke_candidates.extend(main_unchoke.drain()); // Use extend + drain

            // --- Optimistic Unchoke ---
            // Find peers not already selected
            let potential_optimistic: Vec<Peer> = peers
                .iter()
                .filter(|p| !unchoke_candidates.contains(p))
                .cloned()
                .collect();

            if let Some(optimistic_peer) = potential_optimistic.choose(&mut thread_rng()).cloned() {
                debug!(peer.ip = %optimistic_peer.ip, "Selected optimistic unchoke");
                unchoke_candidates.insert(optimistic_peer);
            } else {
                trace!("No candidates left for optimistic unchoke");
            }
        }

        // --- Apply Choke/Unchoke ---
        for (peer, tx) in senders_map.iter() {
            let should_be_unchoked = unchoke_candidates.contains(peer);
            let command: Box<dyn handlers::SwarmCommandHandler + Send> = if should_be_unchoked {
                trace!(peer.ip = %peer.ip, "Sending Unchoke command");
                Box::new(UnchokePeerCommand)
            } else {
                trace!(peer.ip = %peer.ip, "Sending Choke command");
                Box::new(ChokePeerCommand)
            };

            if let Err(e) = tx.send(command) {
                error!(error = %e, peer.ip = %peer.ip, "Failed to send choke/unchoke command");
                // Consider removing peer if channel is closed
            }
        }
        // Drop the lock before sleeping
        drop(senders_map);
    }
    // warn!("Choking loop finished unexpectedly"); // Add if needed
}
