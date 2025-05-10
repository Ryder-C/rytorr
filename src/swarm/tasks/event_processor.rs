use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bit_vec::BitVec;
use tokio::sync::{mpsc::UnboundedReceiver, Mutex, Notify};
use tracing::{debug, info, instrument, trace, warn};

use crate::peer::Peer;
use crate::swarm::state::PeerState;

// --- Peer Event Handling ---

#[async_trait]
pub(crate) trait PeerEventHandler: Send + Sync + std::fmt::Debug {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    );
}

#[derive(Debug)]
pub(crate) struct BitfieldEventHandler {
    pub(crate) peer: Peer,
    pub(crate) bf: BitVec,
}

#[async_trait]
impl PeerEventHandler for BitfieldEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard.entry(self.peer.clone()).or_insert_with(|| {
            debug!(peer.ip = %self.peer.ip, "EventLoop: Creating new state for Bitfield");
            PeerState::new(num_pieces)
        });
        if self.bf.len() == num_pieces {
            st.bitfield = self.bf.clone();
            notify.notify_one(); // State potentially changed
            debug!(peer.ip = %self.peer.ip, num_pieces, "EventLoop: Updated bitfield");
        } else {
            warn!(
                peer.ip = %self.peer.ip,
                received_len = self.bf.len(),
                expected_len = num_pieces,
                "EventLoop: Received bitfield with incorrect length"
            );
        }
    }
}

#[derive(Debug)]
pub(crate) struct HaveEventHandler {
    pub(crate) peer: Peer,
    pub(crate) idx: usize,
}

#[async_trait]
impl PeerEventHandler for HaveEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        _global_have: &Mutex<BitVec>, // Not needed directly, but part of the signature
        notify: &Notify,
    ) {
        let mut states_guard = states.lock().await;
        if let Some(st) = states_guard.get_mut(&self.peer) {
            if st.bitfield.get(self.idx).is_some_and(|b| !b) {
                st.bitfield.set(self.idx, true);
                notify.notify_one(); // State potentially changed
                debug!(peer.ip = %self.peer.ip, piece_index = self.idx, "EventLoop: Set piece");
            } else {
                // Log if already had or index is bad, but don't warn for unknown peer here
                trace!(peer.ip = %self.peer.ip, piece_index = self.idx, "EventLoop: Peer already had piece or index out of bounds");
            }
        } else {
            // If peer is unknown when receiving Have, it's less critical than Bitfield/Choke/Unchoke
            // Might happen if peer disconnects just before event processing.
            debug!(peer.ip = %self.peer.ip, "EventLoop: Received Have for peer not in state map (might have disconnected)");
        }
    }
}

#[derive(Debug)]
pub(crate) struct UnchokeEventHandler {
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for UnchokeEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard.entry(self.peer.clone()).or_insert_with(|| {
            debug!(peer.ip = %self.peer.ip, "EventLoop: Creating new state for Unchoke");
            PeerState::new(num_pieces)
        });
        // This event means the PEER unchoked US.
        if st.peer_is_choking_us {
            st.peer_is_choking_us = false;
            notify.notify_one(); // State changed
            info!(peer.ip = %self.peer.ip, "EventLoop: Registered UNCHOKE from peer (peer_is_choking_us = false)");
        } else {
            trace!(peer.ip = %self.peer.ip, "EventLoop: Peer was already unchoking us");
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChokeEventHandler {
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for ChokeEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        _global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let mut states_guard = states.lock().await;
        if let Some(st) = states_guard.get_mut(&self.peer) {
            // This event means the PEER choked US.
            if !st.peer_is_choking_us {
                st.peer_is_choking_us = true;
                notify.notify_one(); // State changed
                info!(peer.ip = %self.peer.ip, "EventLoop: Registered CHOKE from peer (peer_is_choking_us = true)");
            } else {
                trace!(peer.ip = %self.peer.ip, "EventLoop: Peer was already choking us");
            }
        } else {
            debug!(peer.ip = %self.peer.ip, "EventLoop: Received Choke for peer not in state map (might have disconnected)");
        }
    }
}

// --- New Event Handlers for PeerState ---

#[derive(Debug)]
pub(crate) struct PeerInterestedEventHandler {
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for PeerInterestedEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        if !st.peer_is_interested_in_us {
            st.peer_is_interested_in_us = true;
            notify.notify_one();
            debug!(peer.ip = %self.peer.ip, "EventLoop: Peer is now interested in us");
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeerNotInterestedEventHandler {
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for PeerNotInterestedEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        if st.peer_is_interested_in_us {
            st.peer_is_interested_in_us = false;
            notify.notify_one();
            debug!(peer.ip = %self.peer.ip, "EventLoop: Peer is no longer interested in us");
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalChokeEventHandler {
    // We decided to choke this peer
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for LocalChokeEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        if !st.am_choking_peer {
            st.am_choking_peer = true;
            notify.notify_one();
            debug!(peer.ip = %self.peer.ip, "EventLoop: We are now choking peer");
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalUnchokeEventHandler {
    // We decided to unchoke this peer
    pub(crate) peer: Peer,
}

#[async_trait]
impl PeerEventHandler for LocalUnchokeEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        if st.am_choking_peer {
            st.am_choking_peer = false;
            notify.notify_one();
            debug!(peer.ip = %self.peer.ip, "EventLoop: We are no longer choking peer");
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalInterestUpdateEventHandler {
    // Our interest in peer changed
    pub(crate) peer: Peer,
    pub(crate) am_interested: bool,
}

#[async_trait]
impl PeerEventHandler for LocalInterestUpdateEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        if st.we_are_interested_in_peer != self.am_interested {
            st.we_are_interested_in_peer = self.am_interested;
            notify.notify_one();
            debug!(peer.ip = %self.peer.ip, am_interested = self.am_interested, "EventLoop: Our interest in peer updated");
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeerSentBlockEventHandler {
    // We sent a block to a peer
    pub(crate) peer: Peer,
    pub(crate) bytes: u64,
}

#[async_trait]
impl PeerEventHandler for PeerSentBlockEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        _notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        st.total_uploaded_to_peer += self.bytes;
        // No notify needed, scheduler/choker will read this periodically
        trace!(peer.ip = %self.peer.ip, bytes = self.bytes, total_uploaded = st.total_uploaded_to_peer, "EventLoop: Updated uploaded bytes to peer");
    }
}

#[derive(Debug)]
pub(crate) struct PeerReceivedBlockEventHandler {
    // We received a block from a peer
    pub(crate) peer: Peer,
    pub(crate) bytes: u64,
}

#[async_trait]
impl PeerEventHandler for PeerReceivedBlockEventHandler {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        _notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        let st = states_guard
            .entry(self.peer.clone())
            .or_insert_with(|| PeerState::new(num_pieces));
        st.total_downloaded_from_peer += self.bytes;
        // No notify needed, scheduler/choker will read this periodically
        trace!(peer.ip = %self.peer.ip, bytes = self.bytes, total_downloaded = st.total_downloaded_from_peer, "EventLoop: Updated downloaded bytes from peer");
    }
}

// New event for when a message is sent to a peer
#[derive(Debug)]
pub(crate) struct MessageSentToPeerEvent {
    pub(crate) peer: Peer,
    pub(crate) timestamp: Instant,
}

#[async_trait]
impl PeerEventHandler for MessageSentToPeerEvent {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        _notify: &Notify,
    ) {
        let num_pieces = global_have.lock().await.len();
        let mut states_guard = states.lock().await;
        if let Some(st) = states_guard.get_mut(&self.peer) {
            st.last_message_sent_at = self.timestamp;
            trace!(peer.ip = %self.peer.ip, at = ?self.timestamp, "EventLoop: Updated last_message_sent_at for peer");
        } else {
            // This might happen if peer disconnects and its state is removed right before this event is processed.
            // Or if it's a very new peer and state isn't created yet (though less likely for a *sent* message event).
            // For robustness, we could create the state, but it might be for a peer that's already gone.
            // Let's log it for now.
            debug!(peer.ip = %self.peer.ip, "EventLoop: Received MessageSentToPeerEvent for peer not in state map (might have disconnected)");
            // Optionally, create state if it's critical that this is recorded:
            // let st = states_guard.entry(self.peer.clone()).or_insert_with(|| PeerState::new(num_pieces));
            // st.last_message_sent_at = self.timestamp;
        }
    }
}

// Async task to handle peer events and update state
#[instrument(skip_all)]
pub(crate) async fn event_loop(
    mut evt_rx: UnboundedReceiver<Box<dyn PeerEventHandler + Send>>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    notify: Arc<Notify>,
) {
    while let Some(evt_handler) = evt_rx.recv().await {
        debug!(
            event_type = std::any::type_name_of_val(&*evt_handler),
            "EventLoop: Received event handler"
        );

        // Call the handler's handle method directly
        evt_handler
            .handle(&peer_states, &global_have, &notify)
            .await;
    }
    info!("Event loop finished");
}
