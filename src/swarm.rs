use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use async_channel::{Receiver, Sender};
use bit_vec::BitVec;
use tokio::{
    fs::OpenOptions,
    net::TcpListener,
    sync::{
        mpsc,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore,
    },
    time::{sleep, Duration},
};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

const MAX_CONCURRENT_HANDSHAKES: usize = 10;

use crate::{
    engine::PendingPeer,
    file::Piece,
    peer::{Peer, PeerConnection},
};

#[derive(Debug, Clone)]
pub enum PeerEvent {
    Bitfield(Peer, BitVec),
    Have(Peer, usize),
    Unchoke(Peer),
    Choke(Peer),
}

#[derive(Debug, Clone)]
pub enum SwarmCommand {
    Request(u32, u32, u32),
}

pub struct Swarm {
    peer_reciever: mpsc::Receiver<PendingPeer>,
    channel: (Sender<Piece>, Receiver<Piece>),
    peers: HashSet<Peer>,
    my_id: String,
    torrent_name: String,
    size: u64,
    piece_length: u64,
    pieces: Arc<Vec<[u8; 20]>>,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    peer_cmd_senders: Arc<Mutex<HashMap<Peer, UnboundedSender<SwarmCommand>>>>,
}

// Async task to write pieces to disk and update global have bitfield
// Remove instrument macro to test Send bound issue
// #[instrument(skip(piece_receiver, have, piece_length, num_pieces, file_path))]
async fn disk_writer_loop(
    file_path: String,
    piece_receiver: Receiver<Piece>,
    have: Arc<Mutex<BitVec>>,
    piece_length: u64,
    num_pieces: usize,
) {
    let mut file = match OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&file_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!(path = %file_path, error = %e, "Disk writer failed to open file, exiting task.");
            return;
        }
    };

    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
    while let Ok(piece) = piece_receiver.recv().await {
        let span = tracing::info_span!("disk_write", piece_index = piece.index);

        span.in_scope(|| {
            trace!("Starting disk write operation");
        });

        let offset = piece.index as u64 * piece_length;

        if let Err(e) = file
            .seek(SeekFrom::Start(offset))
            .instrument(span.clone())
            .await
        {
            error!(parent: &span, error = %e, offset, "Disk seek failed");
            continue; // Skip this piece if seek fails
        }
        if let Err(e) = file.write_all(&piece.data).instrument(span.clone()).await {
            error!(parent: &span, error = %e, "Disk write failed");
            continue; // Skip this piece if write fails
        }
        let have_count;
        {
            let mut h = have.lock().instrument(span.clone()).await;
            h.set(piece.index as usize, true);
            have_count = h.iter().filter(|b| *b).count();
        }
        span.in_scope(|| {
            info!(
                have_count = have_count,
                total_pieces = num_pieces,
                "Piece written to disk"
            );
        });
    }
    info!("Disk writer loop finished");
}

/// Per-peer state tracked by the Swarm
#[derive(Clone, Debug)]
struct PeerState {
    bitfield: BitVec,
    is_unchoked: bool,
}

/// Calculates piece rarity across all known peer bitfields.
#[instrument(level = "debug", skip(peer_states, num_pieces))]
fn calculate_rarity(
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

/// The core scheduling loop deciding which blocks to request from whom.
#[instrument(skip_all)]
async fn scheduler_loop(
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    senders: Arc<Mutex<HashMap<Peer, UnboundedSender<SwarmCommand>>>>,
    piece_length: usize,
    num_pieces: usize,
    notify: Arc<Notify>,
) {
    loop {
        // Wait for a state change notification or a timeout (e.g., 1 second)
        tokio::select! {
            _ = notify.notified() => { debug!("Scheduler notified"); }
            _ = sleep(Duration::from_secs(1)) => { debug!("Scheduler timeout"); }
        }

        let states = peer_states.lock().await;
        let have = global_have.lock().await;

        if states.is_empty() {
            debug!("Scheduler: No peers, sleeping.");
            continue;
        }

        let rarity = calculate_rarity(&states, num_pieces);

        let mut requested_this_cycle = false;
        for (piece_idx, _count) in rarity {
            if have.get(piece_idx).unwrap_or(true) {
                continue;
            }

            // Find an unchoked peer that has this piece
            if let Some((peer, _state)) = states
                .iter()
                .find(|(_p, s)| s.is_unchoked && s.bitfield.get(piece_idx).unwrap_or(false))
            {
                debug!(peer.ip = %peer.ip, piece_index = piece_idx, "Found candidate peer for piece");
                // Found a candidate peer, request the first block (for now)
                let length = piece_length as u32;
                let block_size = 16384u32;
                let begin = 0; // TODO: Request blocks sequentially
                let size = if begin + block_size > length {
                    length - begin
                } else {
                    block_size
                };

                // Lock the senders map to get the sender
                let senders_map = senders.lock().await;
                if let Some(tx) = senders_map.get(peer) {
                    debug!(piece_index = piece_idx, begin, size, peer.ip = %peer.ip, "Sending Request");
                    if let Err(e) = tx.send(SwarmCommand::Request(piece_idx as u32, begin, size)) {
                        error!(error = %e, peer.ip = %peer.ip, "Failed to send Request command");
                    }
                    requested_this_cycle = true;
                    break; // Move to next scheduling cycle after finding one block to request
                } else {
                    // This error should be rare if the state is consistent
                    error!(peer.ip = %peer.ip, "Scheduler: Could not find sender for peer in shared map!");
                }
            }
        }
        if !requested_this_cycle {
            debug!("Scheduler: Did not request any blocks this cycle.");
        }
    }
}

// Async task to handle peer events and update state
#[instrument(skip_all)]
async fn event_loop(
    mut evt_rx: UnboundedReceiver<PeerEvent>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    notify: Arc<Notify>,
) {
    while let Some(evt) = evt_rx.recv().await {
        debug!(event = ?evt, "EventLoop: Received event");
        let mut states = peer_states.lock().await;
        let mut state_changed = false;

        match evt {
            PeerEvent::Bitfield(peer, bf) => {
                let num_pieces = global_have.lock().await.len();
                let st = states.entry(peer.clone()).or_insert_with(|| {
                    debug!(peer.ip = %peer.ip, "EventLoop: Creating new state");
                    PeerState {
                        bitfield: BitVec::from_elem(num_pieces, false),
                        is_unchoked: false,
                    }
                });
                if bf.len() == num_pieces {
                    st.bitfield = bf;
                    state_changed = true;
                    debug!(peer.ip = %peer.ip, num_pieces, "EventLoop: Updated bitfield");
                } else {
                    warn!(
                        peer.ip = %peer.ip,
                        received_len = bf.len(),
                        expected_len = num_pieces,
                        "EventLoop: Received bitfield with incorrect length"
                    );
                }
            }
            PeerEvent::Have(peer, idx) => {
                if let Some(st) = states.get_mut(&peer) {
                    if st.bitfield.get(idx).is_some_and(|b| !b) {
                        st.bitfield.set(idx, true);
                        state_changed = true;
                        debug!(peer.ip = %peer.ip, piece_index = idx, "EventLoop: Set piece");
                    } else {
                        debug!(peer.ip = %peer.ip, piece_index = idx, "EventLoop: Peer already had piece or index out of bounds");
                    }
                } else {
                    warn!(peer.ip = %peer.ip, "EventLoop: Received Have for unknown peer");
                }
            }
            PeerEvent::Unchoke(peer) => {
                let num_pieces = global_have.lock().await.len();
                let st = states.entry(peer.clone()).or_insert_with(|| {
                    debug!(peer.ip = %peer.ip, "EventLoop: Creating new state");
                    PeerState {
                        bitfield: BitVec::from_elem(num_pieces, false),
                        is_unchoked: false,
                    }
                });
                if !st.is_unchoked {
                    st.is_unchoked = true;
                    state_changed = true;
                    debug!(peer.ip = %peer.ip, "EventLoop: Unchoked peer");
                } else {
                    debug!(peer.ip = %peer.ip, "EventLoop: Peer was already unchoked");
                }
            }
            PeerEvent::Choke(peer) => {
                if let Some(st) = states.get_mut(&peer) {
                    if st.is_unchoked {
                        st.is_unchoked = false;
                        state_changed = true;
                        debug!(peer.ip = %peer.ip, "EventLoop: Choked peer");
                    } else {
                        debug!(peer.ip = %peer.ip, "EventLoop: Peer was already choked");
                    }
                } else {
                    warn!(peer.ip = %peer.ip, "EventLoop: Received Choke for unknown peer");
                }
            }
        }

        if state_changed {
            debug!("EventLoop: Notifying scheduler");
            notify.notify_one();
        }
    }
    info!("Event loop finished");
}

impl Swarm {
    #[instrument(skip(peer_reciever, pieces, downloaded, uploaded))]
    pub fn new(
        peer_reciever: mpsc::Receiver<PendingPeer>,
        my_id: String,
        torrent_name: String,
        size: u64,
        piece_length: u64,
        pieces: Arc<Vec<[u8; 20]>>,
        downloaded: Arc<RwLock<u64>>,
        uploaded: Arc<RwLock<u64>>,
    ) -> Self {
        info!(
            torrent_name,
            my_id, size, piece_length, "Creating new Swarm"
        );
        Self {
            peer_reciever,
            channel: async_channel::unbounded(),
            peers: HashSet::new(),
            my_id,
            torrent_name,
            size,
            piece_length,
            pieces,
            downloaded,
            uploaded,
            peer_cmd_senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[instrument(skip(self, info_hash))]
    pub async fn start(&mut self, info_hash: Arc<Vec<u8>>) {
        let torrent_name = self.torrent_name.clone();
        let piece_length = self.piece_length;
        let num_pieces = self.pieces.len();
        let have = Arc::new(Mutex::new(BitVec::from_elem(num_pieces, false)));
        let have_clone_disk = have.clone();
        let have_clone_event = have.clone();
        let have_clone_scheduler = have.clone();
        let file_path = format!("{}.download", torrent_name);
        let piece_receiver = self.channel.1.clone();

        info!("Spawning disk writer task");
        // Spawn disk writer task, passing the file path
        tokio::spawn(disk_writer_loop(
            file_path.clone(),
            piece_receiver,
            have_clone_disk,
            piece_length,
            num_pieces,
        ));

        // Shared state for peer information
        let peer_states = Arc::new(Mutex::new(HashMap::<Peer, PeerState>::new()));
        let peer_states_clone_event = peer_states.clone();
        let peer_states_clone_scheduler = peer_states.clone();

        // Notification mechanism for state changes
        let notify = Arc::new(Notify::new());
        let notify_clone_event = notify.clone();
        let notify_clone_scheduler = notify.clone();

        // Central event channel
        let (evt_tx, evt_rx): (UnboundedSender<PeerEvent>, UnboundedReceiver<PeerEvent>) =
            unbounded_channel();
        let senders_arc = self.peer_cmd_senders.clone();

        info!("Spawning event processing task");
        // Spawn event processing task
        tokio::spawn(event_loop(
            evt_rx,
            peer_states_clone_event,
            have_clone_event,
            notify_clone_event,
        ));

        info!("Spawning scheduler task");
        // Spawn the scheduler task
        let piece_length_usize = piece_length as usize;
        tokio::spawn(scheduler_loop(
            peer_states_clone_scheduler,
            have_clone_scheduler,
            senders_arc,
            piece_length_usize,
            num_pieces,
            notify_clone_scheduler,
        ));

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        loop {
            debug!("Waiting for new peer...");
            // Use select to allow graceful shutdown in the future
            let new_peer = match self.peer_reciever.recv().await {
                Some(p) => p,
                None => {
                    info!("Peer receiver channel closed, ending swarm loop.");
                    break;
                }
            };

            let peer_obj = match &new_peer {
                PendingPeer::Incoming(p, _) => p.clone(),
                PendingPeer::Outgoing(p) => p.clone(),
            };
            if self.peers.contains(&peer_obj) {
                debug!(peer.ip = %peer_obj.ip, "Peer already connected");
                continue;
            }
            info!(peer = ?peer_obj, "Received new potential peer");

            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    error!("Semaphore closed unexpectedly");
                    break; // Stop processing if semaphore is closed
                }
            };
            let piece_sender = self.channel.0.clone();
            let (cmd_tx, cmd_rx) = unbounded_channel();
            // Lock the shared map to insert the new sender
            self.peer_cmd_senders
                .lock()
                .await
                .insert(peer_obj.clone(), cmd_tx);

            self.introduce_peer(
                new_peer,
                info_hash.clone(),
                permit,
                piece_sender,
                self.piece_length as usize,
                self.pieces.clone(),
                evt_tx.clone(),
                cmd_rx,
            )
            .await;

            self.peers.insert(peer_obj);
        }
        info!("Swarm start loop finished");
    }

    #[instrument(skip(sender))]
    pub async fn listen_for_peers(sender: mpsc::Sender<PendingPeer>, port: u16) -> Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        info!(local_addr = ?listener.local_addr().ok(), "Listening for incoming peer connections");

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!(peer_addr = %addr, "Incoming connection accepted");
                    let peer = Peer::from_socket_address(addr);
                    if let Err(e) = sender.send(PendingPeer::Incoming(peer, stream)).await {
                        error!(error = %e, "Failed to send incoming peer to swarm");
                        // Decide if we should break the listener loop here
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error accepting incoming connection");
                    // Consider adding a small delay before retrying to avoid tight error loops
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    #[instrument(skip(
        self,
        peer,
        info_hash,
        _permit,
        piece_sender,
        piece_length,
        piece_hashes,
        peer_event_tx,
        cmd_rx
    ))]
    async fn introduce_peer(
        &self,
        peer: PendingPeer,
        info_hash: Arc<Vec<u8>>,
        _permit: OwnedSemaphorePermit,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        peer_event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
    ) {
        let id = self.my_id.clone();
        let peer_ip_for_task = match &peer {
            PendingPeer::Outgoing(p) => p.ip.clone(),
            PendingPeer::Incoming(p, _) => p.ip.clone(),
        };
        let peer_ip_for_span = peer_ip_for_task.clone();

        tokio::spawn(
            async move {
                let peer_ip = peer_ip_for_task;
                info!(peer.ip = %peer_ip, "Attempting connection");
                let mut peer_connection = match PeerConnection::new(
                    peer,
                    id,
                    info_hash,
                    piece_sender,
                    piece_length,
                    piece_hashes,
                    peer_event_tx,
                    cmd_rx,
                )
                .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        warn!(peer.ip = %peer_ip, error = %e, "Failed to establish connection");
                        return;
                    }
                };

                info!(peer = ?peer_connection.peer, "Connection established");

                // The connection loop should handle its own logging
                peer_connection.start().await;

                info!(peer = ?peer_connection.peer, "Connection closed");
                // TODO: Add logic here to remove peer from peer_states and peer_cmd_senders
            }
            .instrument(tracing::info_span!("peer_connection_task", peer.ip = %peer_ip_for_span)),
        );
    }
}
