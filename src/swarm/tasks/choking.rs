use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::peer::Peer;
use crate::swarm::handlers::{self, ChokePeerCommand, UnchokePeerCommand}; // For command handlers
use crate::swarm::state::PeerState; // Import PeerState
use crate::swarm::PeerMapArc;
use std::collections::HashMap;

/// Periodically reviews connected peers and decides which ones to choke/unchoke.
#[instrument(skip_all)]
pub(crate) async fn choking_loop(
    senders: PeerMapArc,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>, // Add peer_states parameter
    upload_slots: usize,
) {
    info!("Starting choking loop with {} upload slots", upload_slots);
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;
        debug!("Evaluating peers for choking/unchoking");

        let mut states_map_guard = peer_states.lock().await;
        let senders_map_guard = senders.lock().await;

        let active_peers: Vec<Peer> = states_map_guard.keys().cloned().collect();
        let peer_count = active_peers.len();
        trace!(
            peer_count,
            "Found peers for choke evaluation from states map"
        );

        if active_peers.is_empty() {
            trace!("No peers to evaluate, skipping choke cycle.");
            drop(states_map_guard); // Release locks
            drop(senders_map_guard);
            continue;
        }

        let mut unchoke_candidates = HashSet::new();

        // Ensure we have at least 1 slot, and optimistic unchoke needs at least 1 slot if active.
        let main_unchoke_slots = if upload_slots > 0 {
            upload_slots.saturating_sub(1)
        } else {
            0
        };
        let enable_optimistic_unchoke = upload_slots > main_unchoke_slots;

        if peer_count <= upload_slots {
            // Unchoke everyone if we have few peers (or equal to slots)
            debug!(
                peer_count,
                max_slots = upload_slots,
                "Unchoking all connected peers (at or below slot limit)"
            );
            for peer in active_peers.iter() {
                if states_map_guard.get(peer).is_some() {
                    // Ensure peer still in states
                    unchoke_candidates.insert(peer.clone());
                }
            }
        } else {
            // --- Tit-for-Tat: Select best uploaders to us who are interested ---
            let mut interested_uploaders: Vec<(Peer, u64)> = Vec::new();
            for peer in active_peers.iter() {
                if let Some(state) = states_map_guard.get(peer) {
                    if state.peer_is_interested_in_us {
                        // Only consider peers interested in our data
                        // Rate is bytes downloaded FROM them in the last interval
                        interested_uploaders.push((
                            peer.clone(),
                            state.download_rate_from_peer_since_last_cycle(),
                        ));
                    }
                }
            }

            // Sort by download rate (descending)
            interested_uploaders.sort_unstable_by(|a, b| b.1.cmp(&a.1));

            debug!(
                count = interested_uploaders.len(),
                "Found interested uploaders for main unchoke selection"
            );

            for (peer, rate) in interested_uploaders.iter().take(main_unchoke_slots) {
                debug!(peer.ip = %peer.ip, rate_Bps_interval = rate, "Selected peer for main unchoke (tit-for-tat)");
                unchoke_candidates.insert(peer.clone());
            }

            // --- Optimistic Unchoke ---
            if enable_optimistic_unchoke {
                let potential_optimistic: Vec<Peer> = active_peers
                    .iter()
                    .filter(|p| {
                        !unchoke_candidates.contains(p) && // Not already chosen for main unchoke
                        states_map_guard.get(p).is_some_and(|s| s.peer_is_interested_in_us)
                        // And interested
                    })
                    .cloned()
                    .collect();

                if !potential_optimistic.is_empty() {
                    if let Some(optimistic_peer) =
                        potential_optimistic.choose(&mut thread_rng()).cloned()
                    {
                        debug!(peer.ip = %optimistic_peer.ip, "Selected optimistic unchoke");
                        unchoke_candidates.insert(optimistic_peer);
                    } else {
                        trace!("No candidates left for optimistic unchoke (random choice failed, should not happen if list not empty)");
                    }
                } else {
                    // If no interested peers are left for optimistic unchoke, try any choked peer
                    let any_choked_for_optimistic: Vec<Peer> = active_peers
                        .iter()
                        .filter(|p| !unchoke_candidates.contains(p))
                        .cloned()
                        .collect();
                    if let Some(optimistic_peer) =
                        any_choked_for_optimistic.choose(&mut thread_rng()).cloned()
                    {
                        debug!(peer.ip = %optimistic_peer.ip, "Selected optimistic unchoke (any choked peer)");
                        unchoke_candidates.insert(optimistic_peer);
                    } else {
                        trace!("No candidates left for optimistic unchoke (all peers already candidates or no peers)");
                    }
                }
            }
        }

        // --- Apply Choke/Unchoke commands and update PeerState.am_choking_peer ---
        for (peer, state) in states_map_guard.iter_mut() {
            // Iterate mutably to update am_choking_peer
            let should_be_unchoked = unchoke_candidates.contains(peer);

            if state.am_choking_peer == should_be_unchoked {
                // Current state is am_choking_peer=true, should_be_unchoked=true (means we need to unchoke)
                // OR am_choking_peer=false, should_be_unchoked=false (means we need to choke)
                let command_to_send: Box<dyn handlers::SwarmCommandHandler + Send>;
                if should_be_unchoked {
                    // We are currently choking them (am_choking_peer=true by deduction), but we want to unchoke them.
                    trace!(peer.ip = %peer.ip, "ChokingLoop: Sending Unchoke command");
                    command_to_send = Box::new(UnchokePeerCommand);
                    state.am_choking_peer = false; // Update our state reflection
                } else {
                    // We are currently not choking them (am_choking_peer=false by deduction), but we want to choke them.
                    trace!(peer.ip = %peer.ip, "ChokingLoop: Sending Choke command");
                    command_to_send = Box::new(ChokePeerCommand);
                    state.am_choking_peer = true; // Update our state reflection
                }

                if let Some(tx) = senders_map_guard.get(peer) {
                    if let Err(e) = tx.send(command_to_send) {
                        error!(error = %e, peer.ip = %peer.ip, "Failed to send choke/unchoke command");
                        // Note: Peer might disconnect, event_processor will eventually remove its state.
                    }
                } else {
                    warn!(peer.ip = %peer.ip, "ChokingLoop: No sender found for peer, cannot send choke/unchoke command.");
                }
            }
            // Snapshot rates for next cycle AFTER decisions but before releasing lock
            state.end_choke_cycle_snapshot();
        }

        // Drop locks before sleeping or next iteration
        drop(states_map_guard);
        drop(senders_map_guard);
    }
    // warn!("Choking loop finished unexpectedly"); // Should not happen
}
