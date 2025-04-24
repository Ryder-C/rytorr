use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use bit_vec::BitVec;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    net::TcpListener,
    sync::{
        mpsc,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore,
    },
    time::sleep,
};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

use crate::{
    engine::PendingPeer,
    file::Piece,
    peer::{Peer, PeerConnection, BLOCK_SIZE},
};

use self::handlers::SwarmCommandHandler;

pub(crate) mod handlers; // Declare the handlers submodule and make it crate-visible

const MAX_CONCURRENT_HANDSHAKES: usize = 10;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PIPELINE_DEPTH: usize = 5; // Max blocks to request consecutively from one peer for one piece

#[derive(Debug, Clone)]
pub enum PeerEvent {
    Bitfield(Peer, BitVec),
    Have(Peer, usize),
    Unchoke(Peer),
    Choke(Peer),
}

pub struct Swarm {
    peer_reciever: mpsc::Receiver<PendingPeer>,
    channel: (Sender<Piece>, Receiver<Piece>),
    my_id: String,
    torrent_name: String,
    piece_length: u64,
    pieces: Arc<Vec<[u8; 20]>>,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    peer_cmd_senders:
        Arc<Mutex<HashMap<Peer, UnboundedSender<Box<dyn handlers::SwarmCommandHandler + Send>>>>>,
    read_file_handle: Option<Arc<Mutex<File>>>,
}

// Async task to write pieces to disk and update global have bitfield
#[instrument(skip(
    piece_receiver,
    have,
    piece_length,
    num_pieces,
    file_path,
    completed_tx
))]
async fn disk_writer_loop(
    file_path: String,
    piece_receiver: Receiver<Piece>,
    have: Arc<Mutex<BitVec>>,
    piece_length: u64,
    num_pieces: usize,
    completed_tx: UnboundedSender<usize>,
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
        // Write the piece to disk
        if let Err(e) = file.write_all(&piece.data).instrument(span.clone()).await {
            error!(parent: &span, error = %e, "Disk write failed");
            continue; // Skip this piece if write fails
        }
        let have_count = {
            let mut h = have.lock().instrument(span.clone()).await;
            h.set(piece.index as usize, true);
            h.iter().filter(|b| *b).count()
        };
        span.in_scope(|| {
            info!(
                have_count = have_count,
                total_pieces = num_pieces,
                "Piece written to disk"
            );
        });
        if let Err(e) = completed_tx.send(piece.index as usize) {
            error!(error = %e, piece_index = piece.index, "Failed to send completed piece notification");
        }
    }
    info!("Disk writer loop finished");
}

/// Per-peer state tracked by the Swarm
#[derive(Clone, Debug)]
pub(crate) struct PeerState {
    bitfield: BitVec,
    is_unchoked: bool,
}

/// Calculates piece rarity across all known peer bitfields.
/// Result is a vector of (piece_index, count) tuples, sorted by rarity.
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
    senders: Arc<
        Mutex<HashMap<Peer, UnboundedSender<Box<dyn handlers::SwarmCommandHandler + Send>>>>,
    >,
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
            _ = sleep(Duration::from_secs(1)) => { debug!("Scheduler timeout"); }
        }

        // --- Timeout check for pending requests ---
        let now = Instant::now();
        let mut pending_requests_guard = pending_requests.lock().await;
        pending_requests_guard.retain(|(idx, begin), requested_at| {
            let elapsed = now.duration_since(*requested_at);
            if elapsed > REQUEST_TIMEOUT {
                warn!(
                    piece_index = idx,
                    block_offset = begin,
                    timeout_secs = REQUEST_TIMEOUT.as_secs(),
                    "Request timed out, removing from pending."
                );
                false // Remove from pending_requests
            } else {
                true // Keep in pending_requests
            }
        });
        drop(pending_requests_guard);

        let states = peer_states.lock().await;
        let have = global_have.lock().await;

        if states.is_empty() {
            debug!("Scheduler: No peers, sleeping.");
            continue;
        }

        let rarity = calculate_rarity(&states, num_pieces);

        let mut requested_this_cycle = false;
        for (piece_idx, _) in rarity {
            if have.get(piece_idx).unwrap_or(true) {
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

            // Find a candidate peer synchronously without locking senders
            let candidate_peer = states.iter().find_map(|(p, s)| {
                if s.is_unchoked && s.bitfield.get(piece_idx).unwrap_or(false) {
                    Some(p.clone())
                } else {
                    None
                }
            });

            // If a candidate is found, asynchronously lock senders and get the tx
            if let Some(peer) = candidate_peer {
                let tx_option = {
                    let senders_map = senders.lock().await; // Use async lock here
                    senders_map.get(&peer).cloned()
                };

                if let Some(tx) = tx_option {
                    debug!(peer.ip = %peer.ip, piece_index = piece_idx, "Found candidate peer for piece");

                    let piece_len_u32 = piece_length as u32;

                    // --- Start Pipelining Logic ---
                    let mut current_begin = {
                        let mut progress_guard = piece_download_progress.lock().await;
                        *progress_guard.entry(piece_idx).or_insert(0)
                    };
                    let mut blocks_requested_for_peer_piece = 0;

                    while blocks_requested_for_peer_piece < PIPELINE_DEPTH {
                        // Check if we have already received all blocks for this piece
                        if current_begin >= piece_len_u32 {
                            trace!(piece_index = piece_idx, "Pipeline: Reached end of piece");
                            break; // Finished this piece
                        }

                        // Check if this specific block is already pending
                        let block_key = (piece_idx, current_begin);
                        let is_pending = {
                            let pending_guard = pending_requests.lock().await;
                            pending_guard.contains_key(&block_key)
                        };

                        if is_pending {
                            trace!(piece_index = piece_idx, block_offset = current_begin, "Pipeline: Block already pending, stopping pipeline for this piece/peer");
                            break; // Stop pipelining for this piece/peer if a block is pending
                        }

                        // Calculate block size, handling the last potentially smaller block
                        let current_size = if current_begin + BLOCK_SIZE as u32 > piece_len_u32 {
                            piece_len_u32 - current_begin
                        } else {
                            BLOCK_SIZE as u32
                        };

                        // Create the handler struct and box it
                        let boxed_handler: Box<dyn handlers::SwarmCommandHandler + Send> =
                            Box::new(handlers::RequestCommandHandler {
                                piece: piece_idx as u32,
                                begin: current_begin,
                                length: current_size,
                            });

                        debug!(piece_index = piece_idx, begin = current_begin, size = current_size, peer.ip = %peer.ip, "Pipeline: Sending Request");
                        if let Err(e) = tx.send(boxed_handler) {
                            error!(error = %e, peer.ip = %peer.ip, piece_index = piece_idx, begin = current_begin, "Pipeline: Failed to send Request command handler, stopping pipeline for this peer/piece");
                            // If send fails, stop trying to pipeline more requests to this peer for this piece now.
                            break;
                        } else {
                            {
                                let mut pending_guard = pending_requests.lock().await;
                                pending_guard.insert(block_key, Instant::now());
                            }
                            requested_this_cycle = true;
                            blocks_requested_for_peer_piece += 1;

                            // Prepare for next potential block in pipeline
                            current_begin += current_size;
                        }
                    } // End of while loop for pipelining

                // NOTE: We DO NOT break the outer loop here.
                // The scheduler can continue to the next piece in the rarity list,
                // potentially finding work for other peers or even the same peer
                // if they have other rare pieces.
                } else {
                    error!(peer.ip = %peer.ip, piece_index = piece_idx, "Scheduler: Could not find sender for found peer!");
                    // Consider removing peer from states if sender is missing, as it indicates inconsistency
                }
            }
        }
        if !requested_this_cycle {
            debug!("Scheduler: Did not request any blocks this cycle.");
        }
    }
}

// --- Peer Event Handling ---

#[async_trait]
pub(crate) trait PeerEventHandler: Send {
    async fn handle(
        &self,
        states: &Mutex<HashMap<Peer, PeerState>>,
        global_have: &Mutex<BitVec>,
        notify: &Notify,
    );
}

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
            PeerState {
                bitfield: BitVec::from_elem(num_pieces, false),
                is_unchoked: false, // Default state
            }
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
            PeerState {
                bitfield: BitVec::from_elem(num_pieces, false),
                is_unchoked: false,
            }
        });
        if !st.is_unchoked {
            st.is_unchoked = true;
            notify.notify_one(); // State changed
            debug!(peer.ip = %self.peer.ip, "EventLoop: Unchoked peer");
        } else {
            trace!(peer.ip = %self.peer.ip, "EventLoop: Peer was already unchoked");
        }
    }
}

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
async fn event_loop(
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

// Task to broadcast Have messages for completed pieces
#[instrument(skip_all)]
async fn broadcast_have_loop(
    mut completed_piece_rx: UnboundedReceiver<usize>,
    senders: Arc<
        Mutex<HashMap<Peer, UnboundedSender<Box<dyn handlers::SwarmCommandHandler + Send>>>>,
    >,
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

impl Swarm {
    #[instrument(skip(peer_reciever, pieces, downloaded, uploaded))]
    pub fn new(
        peer_reciever: mpsc::Receiver<PendingPeer>,
        my_id: String,
        torrent_name: String,
        piece_length: u64,
        pieces: Arc<Vec<[u8; 20]>>,
        downloaded: Arc<RwLock<u64>>,
        uploaded: Arc<RwLock<u64>>,
    ) -> Self {
        info!(torrent_name, my_id, piece_length, "Creating new Swarm");
        Self {
            peer_reciever,
            channel: async_channel::unbounded(),
            my_id,
            torrent_name,
            piece_length,
            pieces,
            downloaded,
            uploaded,
            peer_states: Arc::new(Mutex::new(HashMap::new())),
            peer_cmd_senders: Arc::new(Mutex::new(HashMap::new())),
            read_file_handle: None,
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

        let (completed_piece_tx, completed_piece_rx) = unbounded_channel::<usize>();

        let piece_download_progress = Arc::new(Mutex::new(HashMap::<usize, u32>::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::<(usize, u32), Instant>::new()));

        info!("Spawning disk writer task");
        // Spawn disk writer task, passing the file path and completed piece sender
        tokio::spawn(disk_writer_loop(
            file_path.clone(),
            piece_receiver,
            have_clone_disk,
            piece_length,
            num_pieces,
            completed_piece_tx,
        ));

        // Open file for reading AFTER ensuring disk_writer has potentially created/truncated it
        // Give a small delay to increase likelihood file exists, although ideally we'd sync better.
        sleep(Duration::from_millis(100)).await;
        match File::open(&file_path).await {
            Ok(file) => {
                info!(path = %file_path, "Opened download file for reading by peers");
                self.read_file_handle = Some(Arc::new(Mutex::new(file)));
            }
            Err(e) => {
                error!(path = %file_path, error = %e, "Failed to open download file for reading, uploads will fail.");
                // Swarm can continue, but uploads won't work until file is accessible.
                self.read_file_handle = None;
            }
        }

        // Get clones of shared state for loops
        let peer_states_for_event = self.peer_states.clone(); // Pass to event loop
        let peer_states_for_scheduler = self.peer_states.clone(); // Pass to scheduler

        let notify = Arc::new(Notify::new());
        let notify_clone_event = notify.clone();
        let notify_clone_scheduler = notify.clone();

        let (evt_tx, evt_rx) = unbounded_channel::<Box<dyn PeerEventHandler + Send>>();
        let senders_arc = self.peer_cmd_senders.clone();

        info!("Spawning event processing task");
        // Spawn event processing task
        tokio::spawn(event_loop(
            evt_rx,
            peer_states_for_event,
            have_clone_event,
            notify_clone_event,
        ));

        info!("Spawning scheduler task");
        // Spawn the scheduler task
        let piece_length_usize = piece_length as usize;
        let progress_clone_scheduler = piece_download_progress.clone();
        let pending_clone_scheduler = pending_requests.clone();
        tokio::spawn(scheduler_loop(
            peer_states_for_scheduler,
            have_clone_scheduler,
            senders_arc,
            piece_length_usize,
            num_pieces,
            notify_clone_scheduler,
            progress_clone_scheduler,
            pending_clone_scheduler,
        ));

        info!("Spawning Have broadcast task");
        let senders_clone_broadcast = self.peer_cmd_senders.clone();
        tokio::spawn(broadcast_have_loop(
            completed_piece_rx,
            senders_clone_broadcast,
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
            // Check against peer_cmd_senders map instead of self.peers
            if self.peer_cmd_senders.lock().await.contains_key(&peer_obj) {
                debug!(peer.ip = %peer_obj.ip, "Peer already connected or being connected");
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

            // Optimistically insert sender BEFORE spawning task
            self.peer_cmd_senders
                .lock()
                .await
                .insert(peer_obj.clone(), cmd_tx);

            // Ensure read_file_handle is available
            let read_handle = match &self.read_file_handle {
                Some(handle) => handle.clone(),
                None => {
                    error!(peer.ip=%peer_obj.ip, "Cannot connect peer, read file handle is not available.");
                    // Remove the sender we just added
                    self.peer_cmd_senders.lock().await.remove(&peer_obj);
                    continue; // Skip this peer
                }
            };

            let progress_clone_peer = piece_download_progress.clone();
            let pending_clone_peer = pending_requests.clone();

            self.introduce_peer(
                new_peer,
                info_hash.clone(),
                permit,
                piece_sender,
                self.piece_length as usize,
                self.pieces.clone(),
                self.uploaded.clone(),
                read_handle,
                evt_tx.clone(),
                cmd_rx,
                progress_clone_peer,
                pending_clone_peer,
            )
            .await;
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
        cmd_rx,
        piece_download_progress,
        pending_requests
    ))]
    async fn introduce_peer(
        &self,
        peer: PendingPeer,
        info_hash: Arc<Vec<u8>>,
        _permit: OwnedSemaphorePermit,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        uploaded: Arc<RwLock<u64>>,
        read_file_handle: Arc<Mutex<File>>,
        peer_event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn handlers::SwarmCommandHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) {
        let id = self.my_id.clone();
        let downloaded_counter = self.downloaded.clone();
        // --- Clone shared state for the task --- Need peer_states too
        let peer_cmd_senders_clone = self.peer_cmd_senders.clone();
        let peer_states_clone = self.peer_states.clone();
        // --- End Clone ---

        // Clone peer details early in case PeerConnection::new fails
        let initial_peer_details = match &peer {
            PendingPeer::Outgoing(p) => p.clone(),
            PendingPeer::Incoming(p, _) => p.clone(),
        };
        let peer_ip_for_span = initial_peer_details.ip.clone();

        tokio::spawn(
            async move {
                let peer_ip = initial_peer_details.ip.clone();
                info!(peer.ip = %peer_ip, "Attempting connection");
                let mut peer_connection = match PeerConnection::new(
                    peer, // This moves `peer`
                    id,
                    info_hash,
                    piece_sender,
                    piece_length,
                    piece_hashes,
                    downloaded_counter,
                    uploaded.clone(),
                    read_file_handle.clone(),
                    peer_event_tx,
                    cmd_rx,
                    piece_download_progress,
                    pending_requests,
                )
                .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        warn!(peer.ip = %peer_ip, error = %e, "Failed to establish connection");
                        // Cleanup the sender we added optimistically
                        {
                             let mut senders_map = peer_cmd_senders_clone.lock().await;
                             if senders_map.remove(&initial_peer_details).is_some() {
                                 trace!(peer = ?initial_peer_details, "Removed command sender after connection failure.");
                             } else {
                                 warn!(peer = ?initial_peer_details, "Command sender not found during cleanup after connection failure.");
                             }
                        }
                        return;
                    }
                };

                // Use the peer details from the established connection
                let peer_details = peer_connection.peer.clone();
                info!(peer = ?peer_details, "Connection established, starting loop");

                peer_connection.start().await;

                // --- Cleanup after connection closes ---
                info!(peer = ?peer_details, "Connection closed. Cleaning up...");

                // Remove command sender
                {
                    let mut senders_map = peer_cmd_senders_clone.lock().await;
                    if senders_map.remove(&peer_details).is_some() {
                        trace!(peer = ?peer_details, "Removed command sender.");
                    } else {
                        warn!(peer = ?peer_details, "Command sender not found during cleanup.");
                    }
                }

                // Remove peer state
                {
                    let mut states_map = peer_states_clone.lock().await;
                    if states_map.remove(&peer_details).is_some() {
                         trace!(peer = ?peer_details, "Removed peer state.");
                    } else {
                         warn!(peer = ?peer_details, "Peer state not found during cleanup.");
                    }
                }
                 info!(peer = ?peer_details, "Cleanup finished.");
            }
            .instrument(tracing::info_span!("peer_connection_task", peer.ip = %peer_ip_for_span)),
        );
    }
}
