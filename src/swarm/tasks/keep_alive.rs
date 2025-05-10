use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, trace};

use crate::peer::Peer;
use crate::swarm::handlers::SendKeepAliveCommand;
use crate::swarm::state::PeerState;
use crate::swarm::PeerMapArc;

const KEEP_ALIVE_CHECK_INTERVAL: Duration = Duration::from_secs(30); // How often to check peers
const SEND_KEEP_ALIVE_THRESHOLD: Duration = Duration::from_secs(100); // If no message sent for this long, send KeepAlive (120s is common timeout)

#[instrument(skip_all)]
pub(crate) async fn keep_alive_loop(
    senders: PeerMapArc,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
) {
    info!(
        check_interval_s = KEEP_ALIVE_CHECK_INTERVAL.as_secs(),
        threshold_s = SEND_KEEP_ALIVE_THRESHOLD.as_secs(),
        "Starting KeepAlive loop"
    );
    let mut interval = tokio::time::interval(KEEP_ALIVE_CHECK_INTERVAL);

    loop {
        interval.tick().await;
        trace!("KeepAlive loop evaluating peers");

        let states_map_guard = peer_states.lock().await;
        let senders_map_guard = senders.lock().await;

        if states_map_guard.is_empty() {
            trace!("KeepAlive: No peer states to evaluate, skipping cycle.");
            continue;
        }

        let now = Instant::now();
        for (peer, state) in states_map_guard.iter() {
            if now.duration_since(state.last_message_sent_at) > SEND_KEEP_ALIVE_THRESHOLD {
                debug!(peer.ip = %peer.ip, last_sent_s_ago = now.duration_since(state.last_message_sent_at).as_secs(), "Peer past keep-alive threshold, sending KeepAlive command");
                if let Some(tx) = senders_map_guard.get(peer) {
                    let cmd = Box::new(SendKeepAliveCommand);
                    if let Err(e) = tx.send(cmd) {
                        error!(error = %e, peer.ip = %peer.ip, "Failed to send SendKeepAliveCommand to peer");
                        // Peer task might have died, will be cleaned up eventually
                    }
                } else {
                    // This can happen if peer disconnected and its sender was removed, but state still exists briefly.
                    trace!(peer.ip = %peer.ip, "KeepAlive: No sender found for peer, cannot send KeepAlive. State might be stale.");
                }
            }
        }
    }
    // warn!("KeepAlive loop finished unexpectedly"); // Should not happen in normal operation
}
