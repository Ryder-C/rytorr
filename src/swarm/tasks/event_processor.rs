use std::collections::HashMap;
use std::sync::Arc;

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
        if !st.is_unchoked {
            st.is_unchoked = true;
            notify.notify_one(); // State changed
            info!(peer.ip = %self.peer.ip, "EventLoop: Registered UNCHOKE from peer");
            debug!(peer.ip = %self.peer.ip, "EventLoop: Unchoked peer");
        } else {
            trace!(peer.ip = %self.peer.ip, "EventLoop: Peer was already unchoked");
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
            if st.is_unchoked {
                st.is_unchoked = false;
                notify.notify_one(); // State changed
                info!(peer.ip = %self.peer.ip, "EventLoop: Registered CHOKE from peer");
                debug!(peer.ip = %self.peer.ip, "EventLoop: Choked peer");
            } else {
                trace!(peer.ip = %self.peer.ip, "EventLoop: Peer was already choked");
            }
        } else {
            debug!(peer.ip = %self.peer.ip, "EventLoop: Received Choke for peer not in state map (might have disconnected)");
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
