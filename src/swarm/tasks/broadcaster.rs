use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, error, info, instrument, trace};

use crate::swarm::handlers; // For handlers::SendHaveCommand
use crate::swarm::PeerMapArc;

// Task to broadcast Have messages for completed pieces
#[instrument(skip_all)]
pub(crate) async fn broadcast_have_loop(
    mut completed_piece_rx: UnboundedReceiver<usize>,
    senders: PeerMapArc,
) {
    info!("Starting Have broadcast loop");
    while let Some(piece_index) = completed_piece_rx.recv().await {
        debug!(
            piece_index,
            "Received completed piece notification for broadcast"
        );
        let senders_map = senders.lock().await;
        for (peer, tx) in senders_map.iter() {
            let cmd: Box<dyn handlers::SwarmCommandHandler + Send> =
                Box::new(handlers::SendHaveCommand {
                    piece_index: piece_index as u32,
                });
            if let Err(e) = tx.send(cmd) {
                error!(error = %e, peer.ip = %peer.ip, piece_index, "Failed to send SendHaveCommand to peer");
                // Consider removing the peer's sender if send fails repeatedly
            } else {
                trace!(peer.ip = %peer.ip, piece_index, "Sent SendHaveCommand");
            }
        }
    }
    error!("Have broadcast loop exited unexpectedly");
}
