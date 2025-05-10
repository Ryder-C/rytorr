use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use async_channel::{Receiver, Sender};
use bit_vec::BitVec;
use tokio::{
    fs::File,
    net::TcpListener,
    sync::{
        mpsc,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore, TryAcquireError,
    },
    time::sleep,
};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

use crate::{
    engine::PendingPeer, file::Piece, peer::Peer, swarm::state::PeerState,
    swarm::tasks::event_processor::PeerEventHandler,
};

pub(crate) mod handlers;
pub(crate) mod state;
pub(crate) mod tasks;

// Type aliases (ensure these are not commented out)
pub(crate) type PeerCmdSender = UnboundedSender<Box<dyn handlers::SwarmCommandHandler + Send>>;
pub(crate) type PeerMapArc = Arc<Mutex<HashMap<Peer, PeerCmdSender>>>;

struct BackgroundTasksContext {
    piece_receiver: Receiver<Piece>,
    have_bitfield: Arc<Mutex<BitVec>>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    peer_cmd_senders: PeerMapArc,
    notify_state_change: Arc<Notify>,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    completed_piece_tx: UnboundedSender<usize>,
    completed_piece_rx: UnboundedReceiver<usize>,
    event_rx: UnboundedReceiver<Box<dyn PeerEventHandler + Send>>,
}

pub struct SwarmArgs {
    pub(crate) my_id: String,
    pub(crate) torrent_name: String,
    pub(crate) piece_length: u64,
    pub(crate) pieces: Arc<Vec<[u8; 20]>>,
}

struct PeerLifecycleContext {
    my_id: String,
    info_hash: Arc<Vec<u8>>,
    piece_sender: Sender<Piece>,
    piece_length: usize,
    piece_hashes: Arc<Vec<[u8; 20]>>,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    read_file_handle: Arc<Mutex<File>>,
    peer_event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    peer_cmd_senders_for_cleanup: PeerMapArc,
    peer_states_for_cleanup: Arc<Mutex<HashMap<Peer, PeerState>>>,
}

pub struct SwarmConfig {
    pub(crate) max_peer_connections: usize,
    pub(crate) upload_slots: usize,
    // We can add other swarm-specific tunable parameters here in the future
}

pub struct Swarm {
    config: SwarmConfig,
    peer_reciever: mpsc::Receiver<PendingPeer>,
    channel: (Sender<Piece>, Receiver<Piece>),
    my_id: String,
    torrent_name: String,
    piece_length: u64,
    pieces: Arc<Vec<[u8; 20]>>,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    peer_cmd_senders: PeerMapArc,
    read_file_handle: Option<Arc<Mutex<File>>>,
    connection_semaphore: Arc<Semaphore>,
}

impl Swarm {
    #[instrument(skip(
        peer_reciever,
        args,
        downloaded,
        uploaded,
        peer_cmd_senders,
        config_arg
    ))]
    pub fn new(
        peer_reciever: mpsc::Receiver<PendingPeer>,
        args: SwarmArgs,
        config_arg: SwarmConfig,
        downloaded: Arc<RwLock<u64>>,
        uploaded: Arc<RwLock<u64>>,
        peer_cmd_senders: PeerMapArc,
    ) -> Self {
        info!(torrent_name = %args.torrent_name, my_id = %args.my_id, piece_length = args.piece_length, "Creating new Swarm");
        Self {
            connection_semaphore: Arc::new(Semaphore::new(config_arg.max_peer_connections)),
            config: config_arg,
            peer_reciever,
            channel: async_channel::unbounded(),
            my_id: args.my_id,
            torrent_name: args.torrent_name,
            piece_length: args.piece_length,
            pieces: args.pieces,
            downloaded,
            uploaded,
            peer_states: Arc::new(Mutex::new(HashMap::new())),
            peer_cmd_senders,
            read_file_handle: None,
        }
    }

    #[instrument(skip_all)]
    fn spawn_background_tasks(
        &self,
        ctx: BackgroundTasksContext,
        piece_length_u64: u64,
        num_pieces: usize,
        file_path_for_disk_writer: String,
        upload_slots_for_choking: usize,
    ) {
        info!("Spawning disk writer task");
        tokio::spawn(tasks::disk_io::disk_writer_loop(
            file_path_for_disk_writer,
            ctx.piece_receiver.clone(),
            ctx.have_bitfield.clone(),
            piece_length_u64,
            num_pieces,
            ctx.completed_piece_tx.clone(),
        ));

        info!("Spawning event processing task");
        tokio::spawn(tasks::event_processor::event_loop(
            ctx.event_rx,
            ctx.peer_states.clone(),
            ctx.have_bitfield.clone(),
            ctx.notify_state_change.clone(),
        ));

        info!("Spawning scheduler task");
        let piece_length_usize = piece_length_u64 as usize;
        tokio::spawn(tasks::scheduler::scheduler_loop(
            ctx.peer_states.clone(),
            ctx.have_bitfield.clone(),
            ctx.peer_cmd_senders.clone(),
            piece_length_usize,
            num_pieces,
            ctx.notify_state_change.clone(),
            ctx.piece_download_progress.clone(),
            ctx.pending_requests.clone(),
        ));

        info!("Spawning Have broadcast task");
        tokio::spawn(tasks::broadcaster::broadcast_have_loop(
            ctx.completed_piece_rx,
            ctx.peer_cmd_senders.clone(),
        ));

        info!("Spawning Choking loop");
        tokio::spawn(tasks::choking::choking_loop(
            ctx.peer_cmd_senders.clone(),
            upload_slots_for_choking,
        ));
    }

    #[instrument(skip_all)]
    async fn process_potential_peer(
        &self,
        new_peer: PendingPeer,
        info_hash: Arc<Vec<u8>>,
        evt_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) {
        let peer_obj = match &new_peer {
            PendingPeer::Incoming(p, _) => p.clone(),
            PendingPeer::Outgoing(p) => p.clone(),
        };

        if self.peer_cmd_senders.lock().await.contains_key(&peer_obj) {
            debug!(peer.ip = %peer_obj.ip, "Peer already connected or being connected, skipping.");
            return; // Skip this peer
        }

        match self.connection_semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                info!(peer = ?peer_obj, available_permits = self.connection_semaphore.available_permits(), "Acquired connection permit, processing peer");

                let (cmd_tx, cmd_rx) = unbounded_channel();
                self.peer_cmd_senders
                    .lock()
                    .await
                    .insert(peer_obj.clone(), cmd_tx);

                if self.read_file_handle.is_none() {
                    error!(peer.ip=%peer_obj.ip, "Cannot connect peer, read file handle is not available.");
                    self.peer_cmd_senders.lock().await.remove(&peer_obj);
                    // Permit is dropped here as `permit` goes out of scope if we return
                    return;
                }

                // introduce_peer will get read_file_handle from self and expect it.
                self.introduce_peer(
                    new_peer,
                    permit,
                    cmd_rx,
                    info_hash.clone(),
                    evt_tx.clone(),
                    piece_download_progress.clone(),
                    pending_requests.clone(),
                )
                .await;
            }
            Err(TryAcquireError::NoPermits) => {
                warn!(peer = ?peer_obj, max_connections = self.config.max_peer_connections, "Connection limit reached, dropping peer.");
                // Peer is dropped, permit not acquired
            }
            Err(TryAcquireError::Closed) => {
                // This is a more severe error, perhaps Swarm should stop.
                // For now, just log and the main loop might break if peer_receiver closes.
                error!("Connection semaphore closed unexpectedly!");
            }
        }
    }

    #[instrument(skip(self, info_hash))]
    pub async fn start(&mut self, info_hash: Arc<Vec<u8>>) {
        let torrent_name = self.torrent_name.clone();
        let num_pieces = self.pieces.len();
        let have = Arc::new(Mutex::new(BitVec::from_elem(num_pieces, false)));
        let file_path = format!("{}.download", torrent_name);
        let piece_receiver_for_tasks = self.channel.1.clone();

        let (completed_piece_tx, completed_piece_rx_for_broadcast) = unbounded_channel::<usize>();
        let completed_piece_tx_for_disk = completed_piece_tx.clone();

        let piece_download_progress = Arc::new(Mutex::new(HashMap::<usize, u32>::new()));
        let pending_requests = Arc::new(Mutex::new(HashMap::<(usize, u32), Instant>::new()));

        let notify = Arc::new(Notify::new());
        let (evt_tx, evt_rx_for_event_loop) =
            unbounded_channel::<Box<dyn PeerEventHandler + Send>>();

        let task_ctx = BackgroundTasksContext {
            piece_receiver: piece_receiver_for_tasks,
            have_bitfield: have.clone(),
            peer_states: self.peer_states.clone(),
            peer_cmd_senders: self.peer_cmd_senders.clone(),
            notify_state_change: notify.clone(),
            piece_download_progress: piece_download_progress.clone(),
            pending_requests: pending_requests.clone(),
            completed_piece_tx: completed_piece_tx_for_disk,
            completed_piece_rx: completed_piece_rx_for_broadcast,
            event_rx: evt_rx_for_event_loop,
        };

        let current_upload_slots = self.config.upload_slots;

        self.spawn_background_tasks(
            task_ctx,
            self.piece_length,
            num_pieces,
            file_path.clone(),
            current_upload_slots,
        );

        // Open file for reading AFTER ensuring disk_writer has potentially created/truncated it
        sleep(Duration::from_millis(100)).await;
        match File::open(&file_path).await {
            Ok(file) => {
                info!(path = %file_path, "Opened download file for reading by peers");
                self.read_file_handle = Some(Arc::new(Mutex::new(file)));
            }
            Err(e) => {
                error!(path = %file_path, error = %e, "Failed to open download file for reading, uploads will fail.");
                self.read_file_handle = None;
            }
        }

        // Main loop to accept new peers
        let piece_download_progress_clone = piece_download_progress.clone();
        let pending_requests_clone = pending_requests.clone();
        let info_hash_clone = info_hash.clone(); // Clone for the loop
        let evt_tx_clone = evt_tx.clone(); // Clone for the loop

        // let semaphore = self.connection_semaphore.clone(); // Not needed to clone here anymore
        loop {
            debug!("Waiting for new peer...");

            // Only wait for a peer now, permit handling is in process_potential_peer
            match self.peer_reciever.recv().await {
                Some(new_peer) => {
                    self.process_potential_peer(
                        new_peer,
                        info_hash_clone.clone(), // Clone the Arcs for each call
                        evt_tx_clone.clone(),
                        piece_download_progress_clone.clone(),
                        pending_requests_clone.clone(),
                    )
                    .await;
                }
                None => {
                    info!("Peer receiver channel closed, ending swarm loop.");
                    break;
                }
            }
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

    #[instrument(skip_all)]
    async fn manage_peer_lifecycle(
        initial_peer_details: Peer,
        pending_peer_for_connection: PendingPeer,
        permit: OwnedSemaphorePermit,
        cmd_rx: UnboundedReceiver<Box<dyn handlers::SwarmCommandHandler + Send>>,
        ctx: PeerLifecycleContext,
    ) {
        let _permit = permit;
        let peer_ip_for_logging = initial_peer_details.ip.clone();

        info!(peer.ip = %peer_ip_for_logging, "Attempting connection in manage_peer_lifecycle");

        // Create PeerConnectionArgs for PeerConnection::new
        let pc_args = crate::peer::PeerConnectionArgs {
            my_id: ctx.my_id.clone(),
            info_hash: ctx.info_hash.clone(),
            piece_sender: ctx.piece_sender.clone(),
            piece_length: ctx.piece_length,
            piece_hashes: ctx.piece_hashes.clone(),
            downloaded: ctx.downloaded.clone(),
            uploaded: ctx.uploaded.clone(),
            read_file_handle: ctx.read_file_handle.clone(),
            event_tx: ctx.peer_event_tx.clone(),
            piece_download_progress: ctx.piece_download_progress.clone(),
            pending_requests: ctx.pending_requests.clone(),
        };

        let mut peer_connection = match crate::peer::PeerConnection::new(
            pending_peer_for_connection,
            pc_args,
            cmd_rx,
        )
        .await
        {
            Ok(connection) => connection,
            Err(e) => {
                warn!(peer.ip = %peer_ip_for_logging, error = %e, "Failed to establish connection");
                {
                    let mut senders_map = ctx.peer_cmd_senders_for_cleanup.lock().await;
                    if senders_map.remove(&initial_peer_details).is_some() {
                        trace!(peer = ?initial_peer_details, "Removed command sender after connection failure.");
                    } else {
                        warn!(peer = ?initial_peer_details, "Command sender not found during cleanup after connection failure.");
                    }
                }
                return;
            }
        };

        info!(peer = ?initial_peer_details, "Connection established, starting loop");
        peer_connection.start().await;
        info!(peer = ?initial_peer_details, "Connection closed. Cleaning up...");
        {
            let mut senders_map = ctx.peer_cmd_senders_for_cleanup.lock().await;
            if senders_map.remove(&initial_peer_details).is_some() {
                trace!(peer = ?initial_peer_details, "Removed command sender.");
            } else {
                warn!(peer = ?initial_peer_details, "Command sender not found during cleanup.");
            }
        }
        {
            let mut states_map = ctx.peer_states_for_cleanup.lock().await;
            if states_map.remove(&initial_peer_details).is_some() {
                trace!(peer = ?initial_peer_details, "Removed peer state.");
            } else {
                warn!(peer = ?initial_peer_details, "Peer state not found during cleanup.");
            }
        }
        info!(peer = ?initial_peer_details, "Cleanup finished.");
    }

    #[instrument(skip(
        self,
        pending_peer_arg,
        permit,
        cmd_rx,
        info_hash,
        peer_event_tx,
        piece_download_progress,
        pending_requests
    ))]
    async fn introduce_peer(
        &self,
        pending_peer_arg: PendingPeer,
        permit: OwnedSemaphorePermit,
        cmd_rx: UnboundedReceiver<Box<dyn handlers::SwarmCommandHandler + Send>>,
        info_hash: Arc<Vec<u8>>,
        peer_event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) {
        let initial_peer_details = match &pending_peer_arg {
            PendingPeer::Outgoing(p) => p.clone(),
            PendingPeer::Incoming(p, _) => p.clone(),
        };
        let peer_ip_for_span = initial_peer_details.ip.clone();

        let lifecycle_ctx = PeerLifecycleContext {
            my_id: self.my_id.clone(),
            info_hash,
            piece_sender: self.channel.0.clone(),
            piece_length: self.piece_length as usize,
            piece_hashes: self.pieces.clone(),
            downloaded: self.downloaded.clone(),
            uploaded: self.uploaded.clone(),
            read_file_handle: self
                .read_file_handle
                .clone()
                .expect("Read file handle must be available for introduce_peer"),
            peer_event_tx,
            piece_download_progress,
            pending_requests,
            peer_cmd_senders_for_cleanup: self.peer_cmd_senders.clone(),
            peer_states_for_cleanup: self.peer_states.clone(),
        };

        tokio::spawn(
            Self::manage_peer_lifecycle(
                initial_peer_details,
                pending_peer_arg,
                permit,
                cmd_rx,
                lifecycle_ctx,
            )
            .instrument(tracing::info_span!("peer_connection_task", peer.ip = %peer_ip_for_span)),
        );
    }

    /// Returns a clone of the Arc containing the peer command sender map.
    pub fn get_peer_cmd_senders_arc(&self) -> PeerMapArc {
        self.peer_cmd_senders.clone()
    }
}
