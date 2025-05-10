use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bit_vec::BitVec;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, instrument, trace, warn};

use crate::{
    peer::{Peer, BLOCK_SIZE},
    swarm::{handlers, state::PeerState, PeerCmdSender, PeerMapArc},
};

// Constants moved from swarm.rs
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PIPELINE_DEPTH: usize = 5; // Max blocks to request consecutively from one peer for one piece

/// Calculates piece rarity across all known peer bitfields.
/// Result is a vector of (piece_index, count) tuples, sorted by rarity.
#[instrument(level = "debug", skip(peer_states, num_pieces))]
pub(crate) fn calculate_rarity(
    peer_states: &HashMap<Peer, PeerState>,
    num_pieces: usize,
) -> Vec<(usize, usize)> {
    let mut counts = vec![0; num_pieces];
    for state in peer_states.values() {
        for (idx, has) in state.bitfield.iter().enumerate() {
            if has {
                if let Some(count) = counts.get_mut(idx) {
                    *count += 1;
                }
            }
        }
    }

    let mut rarity: Vec<(usize, usize)> = counts.into_iter().enumerate().collect();
    // Sort by count (ascending), then index (ascending) as a tie-breaker
    rarity.sort_unstable_by(
        |(idx_a, count_a), (idx_b, count_b)| match count_a.cmp(count_b) {
            Ordering::Equal => idx_a.cmp(idx_b),
            other => other,
        },
    );
    debug!(?rarity, "Calculated piece rarity");
    rarity
}

#[instrument(level = "trace", skip(pending_requests, request_timeout_duration))]
async fn handle_request_timeouts(
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    request_timeout_duration: Duration,
) {
    let now = Instant::now();
    let mut pending_requests_guard = pending_requests.lock().await;
    pending_requests_guard.retain(|(idx, begin), requested_at| {
        let elapsed = now.duration_since(*requested_at);
        if elapsed > request_timeout_duration {
            debug!(
                piece_index = idx,
                block_offset = begin,
                timeout_secs = request_timeout_duration.as_secs(),
                "Request timed out, removing from pending."
            );
            false // Remove from pending_requests
        } else {
            true // Keep in pending_requests
        }
    });
    // pending_requests_guard is dropped here
}

#[instrument(level = "trace", skip(tx, piece_download_progress, pending_requests), fields(peer_ip = %peer.ip, piece_idx))]
async fn pipeline_blocks_for_piece_from_peer(
    tx: &PeerCmdSender,
    peer: &Peer,
    piece_idx: usize,
    piece_length_u32: u32,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
) -> bool {
    let mut requested_any_block = false;
    let mut current_begin = {
        let mut progress_guard = piece_download_progress.lock().await;
        *progress_guard.entry(piece_idx).or_insert(0)
    };
    let mut blocks_requested_for_peer_piece = 0;

    while blocks_requested_for_peer_piece < PIPELINE_DEPTH {
        if current_begin >= piece_length_u32 {
            trace!(piece_index = piece_idx, "Pipeline: Reached end of piece");
            break;
        }

        let block_key = (piece_idx, current_begin);
        let is_pending = {
            let pending_guard = pending_requests.lock().await;
            pending_guard.contains_key(&block_key)
        };

        if is_pending {
            trace!(
                piece_index = piece_idx,
                block_offset = current_begin,
                "Pipeline: Block already pending, stopping pipeline for this piece/peer"
            );
            break;
        }

        let current_size = if current_begin + BLOCK_SIZE as u32 > piece_length_u32 {
            piece_length_u32 - current_begin
        } else {
            BLOCK_SIZE as u32
        };

        let boxed_handler: Box<dyn handlers::SwarmCommandHandler + Send> =
            Box::new(handlers::RequestCommandHandler {
                piece: piece_idx as u32,
                begin: current_begin,
                length: current_size,
            });

        debug!(piece_index = piece_idx, begin = current_begin, size = current_size, peer.ip = %peer.ip, "Pipeline: Sending Request");
        if let Err(e) = tx.send(boxed_handler) {
            error!(error = %e, peer.ip = %peer.ip, piece_index = piece_idx, begin = current_begin, "Pipeline: Failed to send Request command handler, stopping pipeline for this peer/piece");
            break;
        } else {
            {
                let mut pending_guard = pending_requests.lock().await;
                pending_guard.insert(block_key, Instant::now());
            }
            requested_any_block = true;
            blocks_requested_for_peer_piece += 1;
            current_begin += current_size;
        }
    }
    requested_any_block
}

#[instrument(
    level = "trace",
    skip(states, senders, piece_download_progress, pending_requests),
    fields(piece_idx)
)]
async fn try_request_blocks_for_piece(
    piece_idx: usize,
    states: &HashMap<Peer, PeerState>,
    senders: PeerMapArc,
    piece_length: usize,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
) -> bool {
    let candidate_peer = states.iter().find_map(|(p, s)| {
        if s.is_unchoked && s.bitfield.get(piece_idx).unwrap_or(false) {
            Some(p.clone())
        } else {
            None
        }
    });

    if let Some(peer) = candidate_peer {
        let tx_option = {
            let senders_map = senders.lock().await;
            senders_map.get(&peer).cloned()
        };

        if let Some(tx) = tx_option {
            if let Some(peer_state) = states.get(&peer) {
                trace!(peer.ip = %peer.ip, peer.state = ?peer_state, piece_index = piece_idx, "Scheduler: Sending request to selected peer.");
            } else {
                warn!(peer.ip = %peer.ip, piece_index = piece_idx, "Scheduler: Peer state not found just before sending request!");
            }
            debug!(peer.ip = %peer.ip, piece_index = piece_idx, "Found candidate peer for piece");

            let piece_len_u32 = piece_length as u32;
            return pipeline_blocks_for_piece_from_peer(
                &tx,
                &peer,
                piece_idx,
                piece_len_u32,
                piece_download_progress,
                pending_requests,
            )
            .await;
        } else {
            error!(peer.ip = %peer.ip, piece_index = piece_idx, "Scheduler: Could not find sender for found peer!");
        }
    }
    false
}

/// The core scheduling loop deciding which blocks to request from whom.
#[instrument(skip_all)]
pub(crate) async fn scheduler_loop(
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    senders: PeerMapArc,
    piece_length: usize,
    num_pieces: usize,
    notify: Arc<Notify>,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
) {
    loop {
        // Wait for a state change notification or a timeout (e.g., 1 second)
        tokio::select! {
            _ = notify.notified() => { debug!("Scheduler notified"); }
            _ = tokio::time::sleep(Duration::from_secs(1)) => { debug!("Scheduler timeout"); }
        }

        // --- Timeout check for pending requests ---
        handle_request_timeouts(pending_requests.clone(), REQUEST_TIMEOUT).await;

        let states_guard = peer_states.lock().await;
        let have_guard = global_have.lock().await;

        if states_guard.is_empty() {
            debug!("Scheduler: No peers, sleeping.");
            // Release locks before continuing
            drop(states_guard);
            drop(have_guard);
            continue;
        }

        let rarity = calculate_rarity(&states_guard, num_pieces);

        // Release locks now that we have rarity and can work with cloned Arcs for requests
        drop(states_guard);
        drop(have_guard);

        let mut requested_this_cycle = false;
        for (piece_idx, _) in rarity {
            // Re-check have status for the specific piece inside the loop, as it might change
            let current_have = global_have.lock().await;
            if current_have.get(piece_idx).unwrap_or(true) {
                drop(current_have); // release lock
                {
                    let mut progress_guard = piece_download_progress.lock().await;
                    progress_guard.remove(&piece_idx);
                }
                {
                    let mut pending_requests_guard = pending_requests.lock().await;
                    pending_requests_guard.retain(|(p_idx, _), _| *p_idx != piece_idx);
                }
                continue;
            }
            drop(current_have); // release lock

            // We need to pass a reference to the states map for `try_request_blocks_for_piece`
            // Lock it again briefly. This is a bit of lock contention, could be optimized
            // if `try_request_blocks_for_piece` took individual peer states or if rarity calculation
            // also returned candidate peers. For now, this is simpler.
            let states_for_request = peer_states.lock().await;
            if try_request_blocks_for_piece(
                piece_idx,
                &states_for_request, // Pass as reference
                senders.clone(),
                piece_length,
                piece_download_progress.clone(),
                pending_requests.clone(),
            )
            .await
            {
                requested_this_cycle = true;
            }
            drop(states_for_request); // Release lock
                                      // NOTE: We DO NOT break the outer loop here.
                                      // The scheduler can continue to the next piece in the rarity list,
                                      // potentially finding work for other peers or even the same peer
                                      // if they have other rare pieces.
        }
        if !requested_this_cycle {
            debug!("Scheduler: Did not request any blocks this cycle.");
        }
    }
}
