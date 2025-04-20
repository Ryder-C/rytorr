use std::{
    collections::{HashSet, HashMap},
    sync::Arc,
    cmp::Ordering,
};

use anyhow::Result;
use async_channel::{Receiver, Sender};
use bit_vec::BitVec;
use tokio::{
    fs::OpenOptions,
    net::TcpListener,
    sync::{Mutex, Notify, mpsc, OwnedSemaphorePermit, RwLock, Semaphore, mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel}},
    time::{sleep, Duration},
};

const MAX_CONCURRENT_HANDSHAKES: usize = 10;

use crate::{
    client::PendingPeer,
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
    pieces: Vec<[u8; 20]>,
    downloaded: Arc<RwLock<u64>>,
    uploaded: Arc<RwLock<u64>>,
    peer_cmd_senders: HashMap<Peer, UnboundedSender<SwarmCommand>>,
}

// Async task to write pieces to disk and update global have bitfield
async fn disk_writer_loop(
    mut file: tokio::fs::File,
    mut piece_receiver: Receiver<Piece>,
    have: Arc<Mutex<BitVec>>,
    piece_length: u64,
    num_pieces: usize,
) {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
    while let Ok(piece) = piece_receiver.recv().await {
        let offset = piece.index as u64 * piece_length;
        file.seek(SeekFrom::Start(offset))
            .await
            .expect("seek failed");
        file.write_all(&piece.data).await.expect("write failed");
        let mut h = have.lock().await;
        h.set(piece.index as usize, true);
        println!(
            "Piece {} written ({}/{})",
            piece.index,
            h.iter().filter(|b| *b).count(),
            num_pieces
        );
    }
}

/// Per-peer state tracked by the Swarm
#[derive(Clone)]
struct PeerState {
    bitfield: BitVec,
    is_unchoked: bool,
}

/// Calculates piece rarity across all known peer bitfields.
/// Returns a sorted list of (piece_index, count) tuples, rarest first.
fn calculate_rarity(peer_states: &HashMap<Peer, PeerState>, num_pieces: usize) -> Vec<(usize, usize)> {
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
    rarity.sort_unstable_by(|(idx_a, count_a), (idx_b, count_b)| {
        match count_a.cmp(count_b) {
            Ordering::Equal => idx_a.cmp(idx_b),
            other => other,
        }
    });
    rarity
}

/// The core scheduling loop deciding which blocks to request from whom.
async fn scheduler_loop(
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    senders: HashMap<Peer, UnboundedSender<SwarmCommand>>,
    piece_length: usize,
    num_pieces: usize,
    notify: Arc<Notify>,
) {
    loop {
        // Wait for a state change notification or a timeout (e.g., 1 second)
        tokio::select! {
            _ = notify.notified() => { /* State changed, proceed to schedule */ }
            _ = sleep(Duration::from_secs(1)) => { /* Timeout, proceed to schedule */ }
        }

        let states = peer_states.lock().await;
        if states.is_empty() {
            // No peers, nothing to do
            continue;
        }
        let have = global_have.lock().await;

        let rarity = calculate_rarity(&states, num_pieces);

        // Simple strategy: Iterate through rarest pieces we don't have
        // and request the first available block from the first available peer.
        for (piece_idx, _count) in rarity {
            if have.get(piece_idx).unwrap_or(true) {
                // We already have this piece
                continue;
            }

            // Find an unchoked peer that has this piece
            if let Some((peer, _state)) = states.iter().find(|(_p, s)| s.is_unchoked && s.bitfield.get(piece_idx).unwrap_or(false)) {
                // Found a candidate peer, request the first block (for now)
                let length = piece_length as u32;
                let block_size = 16384u32;
                let begin = 0; // TODO: Request blocks sequentially
                let size = if begin + block_size > length { length - begin } else { block_size };

                if let Some(tx) = senders.get(peer) {
                    println!("Scheduler requesting piece {} block 0 from {}", piece_idx, peer.ip);
                    let _ = tx.send(SwarmCommand::Request(piece_idx as u32, begin, size));
                    break; // Move to next scheduling cycle after finding one block to request
                }
            }
        }
    } // states and have go out of scope here in the normal path, dropping automatically
}

// Async task to handle peer events and update state
async fn event_loop(
    mut evt_rx: UnboundedReceiver<PeerEvent>,
    peer_states: Arc<Mutex<HashMap<Peer, PeerState>>>,
    global_have: Arc<Mutex<BitVec>>,
    notify: Arc<Notify>,
) {
    while let Some(evt) = evt_rx.recv().await {
        println!("Received event: {:?}", evt);
        let mut states = peer_states.lock().await;
        let mut state_changed = false;
        match evt {
            PeerEvent::Bitfield(peer, bf) => {
                let num_pieces = global_have.lock().await.len(); // Get expected length
                let st = states.entry(peer.clone()).or_insert_with(|| PeerState {
                    bitfield: BitVec::from_elem(num_pieces, false),
                    is_unchoked: false, // Assume choked initially
                });
                // Ensure bitfield has the correct length, resizing if necessary
                if bf.len() == num_pieces {
                    st.bitfield = bf;
                    state_changed = true;
                } else {
                    println!("Warning: Received bitfield with incorrect length from {}", peer.ip);
                }
            }
            PeerEvent::Have(peer, idx) => {
                if let Some(st) = states.get_mut(&peer) {
                    if st.bitfield.get(idx).is_some_and(|b| !b) {
                        st.bitfield.set(idx, true);
                        state_changed = true;
                    }
                }
            }
            PeerEvent::Unchoke(peer) => {
                let num_pieces = global_have.lock().await.len();
                let st = states.entry(peer.clone()).or_insert_with(|| PeerState {
                    bitfield: BitVec::from_elem(num_pieces, false),
                    is_unchoked: false,
                });
                if !st.is_unchoked {
                    st.is_unchoked = true;
                    state_changed = true;
                }
            }
            PeerEvent::Choke(peer) => {
                if let Some(st) = states.get_mut(&peer) {
                    if st.is_unchoked {
                        st.is_unchoked = false;
                        state_changed = true;
                    }
                }
            }
        }

        if state_changed {
            notify.notify_one();
        }
    } // states goes out of scope here, dropping automatically
}

impl Swarm {
    pub fn new(
        peer_reciever: mpsc::Receiver<PendingPeer>,
        my_id: String,
        torrent_name: String,
        size: u64,
        piece_length: u64,
        pieces: Vec<[u8; 20]>,
        downloaded: Arc<RwLock<u64>>,
        uploaded: Arc<RwLock<u64>>,
    ) -> Self {
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
            peer_cmd_senders: HashMap::new(),
        }
    }

    pub async fn start(&mut self, info_hash: &'static [u8]) {
        let torrent_name = self.torrent_name.clone();
        let piece_length = self.piece_length;
        let num_pieces = self.pieces.len();
        let have = Arc::new(Mutex::new(BitVec::from_elem(num_pieces, false)));
        let have_clone_disk = have.clone();
        let have_clone_event = have.clone();
        let have_clone_scheduler = have.clone();
        let file_path = format!("{}.download", torrent_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_path)
            .await
            .expect("Failed to open file");

        let piece_receiver = self.channel.1.clone();

        // Spawn disk writer task
        tokio::spawn(disk_writer_loop(
            file,
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
        let (evt_tx, evt_rx): (UnboundedSender<PeerEvent>, UnboundedReceiver<PeerEvent>) = unbounded_channel();
        let senders = self.peer_cmd_senders.clone();
        let senders_clone_scheduler = senders.clone();

        // Spawn event processing task
        tokio::spawn(event_loop(
            evt_rx,
            peer_states_clone_event,
            have_clone_event,
            notify_clone_event,
        ));

        // Spawn the scheduler task
        let piece_length_usize = piece_length as usize;
        tokio::spawn(scheduler_loop(
            peer_states_clone_scheduler,
            have_clone_scheduler,
            senders_clone_scheduler,
            piece_length_usize,
            num_pieces,
            notify_clone_scheduler,
        ));

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        loop {
            println!("Waiting for new peer...");
            let new_peer = self.peer_reciever.recv().await.unwrap();

            let peer_obj = match &new_peer {
                PendingPeer::Incoming(p, _) => p.clone(),
                PendingPeer::Outgoing(p) => p.clone(),
            };
            if self.peers.contains(&peer_obj) {
                println!("Peer already connected: {:?}", peer_obj);
                if let PendingPeer::Incoming(_, stream) = new_peer {
                    drop(stream);
                }
                continue;
            }
            println!("Adding new peer: {:?}", new_peer);

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let piece_sender = self.channel.0.clone();
            let (cmd_tx, cmd_rx) = unbounded_channel();
            self.peer_cmd_senders.insert(peer_obj.clone(), cmd_tx);

            self.introduce_peer(
                new_peer,
                info_hash,
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
    }

    pub async fn listen_for_peers(sender: mpsc::Sender<PendingPeer>, port: u16) -> Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("Incoming connection from: {:?}", addr);
            let peer = Peer::from_socket_address(addr);

            sender
                .send(PendingPeer::Incoming(peer, stream))
                .await
                .unwrap();
        }
    }

    async fn introduce_peer(
        &self,
        peer: PendingPeer,
        info_hash: &'static [u8],
        _permit: OwnedSemaphorePermit,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Vec<[u8; 20]>,
        peer_event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
    ) {
        let id = self.my_id.clone();
        tokio::spawn(async move {
            let peer_ip = match &peer {
                PendingPeer::Outgoing(p) => p.ip.clone(),
                PendingPeer::Incoming(p, _) => p.ip.clone(),
            };
            println!("Attempting connection with peer: {}", peer_ip);
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
                    println!("Failed to establish connection with {}: {:?}", peer_ip, e);
                    return;
                }
            };

            println!(
                "Connection established with peer: {:?}",
                peer_connection.peer
            );

            peer_connection.start().await;

            println!("Connection closed with peer: {:?}", peer_connection.peer);
        });
    }
}
