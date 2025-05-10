mod handlers;
pub(crate) mod message;

use std::collections::HashMap;
use std::time::Instant;
use std::{hash::Hash, net::SocketAddr, sync::Arc};

use crate::engine::PendingPeer;
use crate::file::Piece;
use crate::swarm::tasks::event_processor::{
    LocalChokeEventHandler, LocalInterestUpdateEventHandler, LocalUnchokeEventHandler,
    MessageSentToPeerEvent, PeerEventHandler,
};
use anyhow::{ensure, Context, Result};
use async_channel::Sender;
use bendy::decoding::FromBencode;
use bit_vec::BitVec;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    sync::{Mutex, RwLock},
};
use tracing::{debug, error, info, trace, warn};

use crate::peer::handlers::MessageHandler;
use crate::peer::message::from_bytes_to_handler;
use crate::peer::message::Message;
use crate::swarm::handlers::SwarmCommandHandler;

pub const BLOCK_SIZE: usize = 16384;

#[derive(Debug, Clone)]
pub struct Peer {
    pub peer_id: Option<String>,
    pub ip: String,
    pub port: u16,
}

impl Peer {
    pub fn from_be_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == 6, "can only decode peer from 6 bytes");

        let ip = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);
        Ok(Self {
            peer_id: None,
            ip,
            port,
        })
    }

    pub fn from_socket_address(addr: SocketAddr) -> Self {
        Self {
            peer_id: None,
            ip: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}

impl Eq for Peer {}

impl Hash for Peer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
    }
}

impl FromBencode for Peer {
    fn decode_bencode_object(
        object: bendy::decoding::Object,
    ) -> Result<Self, bendy::decoding::Error>
    where
        Self: Sized,
    {
        let mut peer_id = None;
        let mut ip = None;
        let mut port = None;

        let mut dict = object.try_into_dictionary()?;

        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"peer id", value) => peer_id = Some(String::decode_bencode_object(value)?),
                (b"ip", value) => ip = Some(String::decode_bencode_object(value)?),
                (b"port", value) => port = Some(u16::decode_bencode_object(value)?),
                _ => {}
            }
        }
        let ip = ip.ok_or_else(|| bendy::decoding::Error::missing_field("ip"))?;
        let port = port.ok_or_else(|| bendy::decoding::Error::missing_field("port"))?;

        Ok(Self { peer_id, ip, port })
    }
}

// New struct for PeerConnection arguments - make it pub(crate)
pub(crate) struct PeerConnectionArgs {
    pub(crate) my_id: String,
    pub(crate) info_hash: Arc<Vec<u8>>,
    pub(crate) piece_sender: Sender<Piece>,
    pub(crate) piece_length: usize,
    pub(crate) piece_hashes: Arc<Vec<[u8; 20]>>,
    pub(crate) downloaded: Arc<RwLock<u64>>,
    pub(crate) uploaded: Arc<RwLock<u64>>,
    pub(crate) read_file_handle: Arc<Mutex<File>>,
    pub(crate) event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
    pub(crate) piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pub(crate) pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
}

#[derive(Debug, Clone, Copy)]
struct ConnectionStateFlags {
    pub(crate) am_choking: bool,
    pub(crate) am_interested: bool,
    pub(crate) peer_choking: bool,
    pub(crate) peer_interested: bool,
}

impl ConnectionStateFlags {
    fn new() -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }
}

pub struct PeerConnection {
    pub peer: Peer,
    pub(crate) piece_sender: Sender<Piece>,
    pub(crate) piece_length: usize,
    pub(crate) piece_hashes: Arc<Vec<[u8; 20]>>,
    pub(crate) pending_pieces: HashMap<usize, Vec<Option<Vec<u8>>>>,
    pub(crate) my_id: String,
    stream: TcpStream,
    pub peer_bitfield: Option<BitVec>,
    pub have_bitfield: Option<BitVec>,

    pub(crate) connection_state: ConnectionStateFlags,

    pub(crate) event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
    cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,

    pub(crate) downloaded: Arc<RwLock<u64>>,
    pub(crate) uploaded: Arc<RwLock<u64>>,
    pub(crate) read_file_handle: Arc<Mutex<File>>,
    pub(crate) piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pub(crate) pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
}

// --- PeerConnection Implementation ---
impl PeerConnection {
    const PSTR: &'static str = "BitTorrent protocol";

    pub async fn new(
        peer: PendingPeer,
        args: PeerConnectionArgs,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
    ) -> Result<Self> {
        match peer {
            PendingPeer::Outgoing(p) => Self::new_outgoing(p, args, cmd_rx).await,
            PendingPeer::Incoming(p, s) => Self::new_incoming(p, s, args, cmd_rx).await,
        }
    }

    async fn new_outgoing(
        peer: Peer,
        args: PeerConnectionArgs,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .context("Connect to TcpStream")?;
        Self::write_handshake(&mut stream, &args.info_hash, &args.my_id).await?;
        Self::read_handshake(&mut stream, &args.info_hash).await?;
        let have = BitVec::from_elem(args.piece_hashes.len(), false);
        Ok(Self {
            peer,
            piece_sender: args.piece_sender,
            piece_length: args.piece_length,
            piece_hashes: args.piece_hashes,
            pending_pieces: HashMap::new(),
            my_id: args.my_id,
            stream,
            peer_bitfield: None,
            have_bitfield: Some(have),
            connection_state: ConnectionStateFlags::new(),
            event_tx: args.event_tx,
            cmd_rx,
            downloaded: args.downloaded,
            uploaded: args.uploaded,
            read_file_handle: args.read_file_handle,
            piece_download_progress: args.piece_download_progress,
            pending_requests: args.pending_requests,
        })
    }

    async fn new_incoming(
        peer: Peer,
        mut stream: TcpStream,
        args: PeerConnectionArgs,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
    ) -> Result<Self> {
        Self::read_handshake(&mut stream, &args.info_hash).await?;
        Self::write_handshake(&mut stream, &args.info_hash, &args.my_id).await?;
        let have = BitVec::from_elem(args.piece_hashes.len(), false);
        Ok(Self {
            peer,
            piece_sender: args.piece_sender,
            piece_length: args.piece_length,
            piece_hashes: args.piece_hashes,
            pending_pieces: HashMap::new(),
            my_id: args.my_id,
            stream,
            peer_bitfield: None,
            have_bitfield: Some(have),
            connection_state: ConnectionStateFlags::new(),
            event_tx: args.event_tx,
            cmd_rx,
            downloaded: args.downloaded,
            uploaded: args.uploaded,
            read_file_handle: args.read_file_handle,
            piece_download_progress: args.piece_download_progress,
            pending_requests: args.pending_requests,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn start(&mut self) {
        // Send initial Bitfield (if we have pieces)
        if let Some(ref have) = self.have_bitfield {
            if !have.is_empty() && have.any() {
                // Only send if not empty and has pieces
                debug!("Sending initial Bitfield");
                if let Err(e) = self.send_message(Message::Bitfield(have.clone())).await {
                    error!(error = %e, "Failed to send initial Bitfield");
                    return; // Can't proceed without sending bitfield if we have pieces
                }
            } else {
                trace!("Skipping initial Bitfield (empty or no pieces)");
            }
        } else {
            trace!("Skipping initial Bitfield (not initialized)");
        }

        // Send initial Interested state update (which might send Interested or NotInterested)
        debug!("Updating initial interest state");
        if let Err(e) = self.update_interest_state().await {
            error!(error = %e, "Failed to send initial interest state message");
            // Decide if we should return or continue
        }

        let mut len_buf = [0u8; 4];
        loop {
            trace!("Peer loop waiting for command or message");
            tokio::select! {
                // Biased select ensures we prioritize sending commands over reading messages if both are ready
                // This might be desirable for responsiveness to requests
                biased;
                Some(cmd_handler) = self.cmd_rx.recv() => {
                    cmd_handler.handle(self).await;
                }
                read_result = self.stream.read_exact(&mut len_buf) => {
                    match read_result {
                        Ok(_) => {
                            let len = u32::from_be_bytes(len_buf);
                            trace!(message_len = len, "Received message length");
                            if len == 0 {
                                debug!("Received KeepAlive");
                                handlers::KeepAliveHandler.handle(self).await;
                                continue;
                            }
                            let mut msg_buf = vec![0u8; len as usize];
                            if let Err(e) = self.stream.read_exact(&mut msg_buf).await {
                                error!(error = %e, "Failed to read message body");
                                break; // Assume connection is broken
                            }

                            match from_bytes_to_handler(&msg_buf) {
                                Ok(handler) => {
                                    handler.handle(self).await;
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to parse message into handler");
                                    // Decide whether to break or continue, e.g., break;
                                }
                            }
                        },
                        Err(e) => {
                            info!(error = %e, "Connection closed by peer or read error");
                            break; // Exit loop on read error
                        }
                    }
                }
            }
        }
        info!(peer.ip = %self.peer.ip, "Peer connection loop finished");
    }

    #[tracing::instrument(skip(self, message), fields(message_type = ?message.to_str()))]
    pub(crate) async fn send_message(&mut self, message: message::Message) -> Result<()> {
        let payload = message.to_be_bytes();
        self.stream.write_all(&payload).await?;

        // After successful send, emit event to update last_message_sent_at
        let event: Box<dyn PeerEventHandler + Send> = Box::new(MessageSentToPeerEvent {
            peer: self.peer.clone(),
            timestamp: Instant::now(),
        });
        if self.event_tx.send(event).is_err() {
            // Log error, but don't let this fail the send_message operation itself
            error!(peer.ip = %self.peer.ip, "Failed to send MessageSentToPeerEvent to event loop");
        }

        Ok(())
    }

    #[tracing::instrument(skip(stream, info_hash, peer_id))]
    async fn write_handshake(
        stream: &mut TcpStream,
        info_hash: &[u8],
        peer_id: &str,
    ) -> Result<()> {
        let mut buf = [0u8; 49 + Self::PSTR.len()];
        buf[0] = Self::PSTR.len() as u8;
        buf[1..(1 + Self::PSTR.len())].copy_from_slice(Self::PSTR.as_bytes());
        buf[(1 + Self::PSTR.len())..(1 + Self::PSTR.len() + 8)].copy_from_slice(&[0u8; 8]); // Reserved 8 bytes
        buf[(1 + Self::PSTR.len() + 8)..(1 + Self::PSTR.len() + 8 + info_hash.len())]
            .copy_from_slice(info_hash); // Info hash 20 bytes
        buf[(1 + Self::PSTR.len() + 8 + info_hash.len())
            ..(1 + Self::PSTR.len() + 8 + info_hash.len() + peer_id.len())]
            .copy_from_slice(peer_id.as_bytes());

        stream.write_all(&buf).await.context("Write handshake")
    }

    #[tracing::instrument(skip(stream, info_hash))]
    pub async fn read_handshake(stream: &mut TcpStream, info_hash: &[u8]) -> Result<()> {
        let mut response = [0u8; 49 + Self::PSTR.len()];
        stream
            .read_exact(&mut response)
            .await
            .context("Read handshake")?;

        ensure!(
            response[0] as usize == Self::PSTR.len(),
            "Peer did not send correct pstrlen"
        );
        ensure!(
            &response[1..(1 + Self::PSTR.len())] == Self::PSTR.as_bytes(),
            "Peer did not send correct pstr"
        );
        ensure!(
            &response[(1 + Self::PSTR.len() + 8)..(1 + Self::PSTR.len() + 8 + 20)] == info_hash,
            "Peer did not send correct info hash"
        );

        Ok(())
    }

    /// Determines if we should be interested in the peer based on their bitfield and ours.
    fn should_i_be_interested(&self) -> bool {
        match (&self.have_bitfield, &self.peer_bitfield) {
            (Some(have_bf), Some(peer_bf)) => {
                // Ensure bitfields are comparable
                if have_bf.len() != peer_bf.len() {
                    warn!(
                        my_len = have_bf.len(),
                        peer_len = peer_bf.len(),
                        "Bitfield length mismatch, cannot determine interest accurately."
                    );
                    // Default to not interested if lengths mismatch, or handle as error?
                    return false;
                }

                // Iterate through pieces. Are we interested if the peer HAS a piece we DON'T HAVE?
                for i in 0..have_bf.len() {
                    if peer_bf.get(i).unwrap_or(false) && !have_bf.get(i).unwrap_or(false) {
                        // Peer has piece 'i', we do not. We are interested.
                        return true;
                    }
                }
                // If loop completes, peer has no pieces we need.
                false
            }
            _ => {
                // If either bitfield is missing, we can't determine interest.
                trace!("Cannot determine interest: one or both bitfields missing.");
                false
            }
        }
    }

    /// Checks interest state and sends Interested or NotInterested message if it changed.
    pub(crate) async fn update_interest_state(&mut self) -> Result<()> {
        let should_be_interested = self.should_i_be_interested();

        if !self.connection_state.am_interested && should_be_interested {
            self.connection_state.am_interested = true;
            trace!("Becoming interested in peer {}", self.peer.ip);
            self.send_message(Message::Interested).await?;
            let event: Box<dyn PeerEventHandler + Send> =
                Box::new(LocalInterestUpdateEventHandler {
                    peer: self.peer.clone(),
                    am_interested: true,
                });
            if self.event_tx.send(event).is_err() {
                error!(peer.ip = %self.peer.ip, "Failed to send LocalInterestUpdateEvent (true)");
            }
        } else if self.connection_state.am_interested && !should_be_interested {
            self.connection_state.am_interested = false;
            trace!("Becoming not interested in peer {}", self.peer.ip);
            self.send_message(Message::NotInterested).await?;
            let event: Box<dyn PeerEventHandler + Send> =
                Box::new(LocalInterestUpdateEventHandler {
                    peer: self.peer.clone(),
                    am_interested: false,
                });
            if self.event_tx.send(event).is_err() {
                error!(peer.ip = %self.peer.ip, "Failed to send LocalInterestUpdateEvent (false)");
            }
        }
        // No change needed if state matches calculation
        Ok(())
    }

    /// Set choking state to true and send Choke message.
    /// Called externally (e.g., by Swarm) based on choking algorithm.
    // Make crate-visible so SwarmCommandHandler can call it
    pub(crate) async fn set_choking(&mut self) -> Result<()> {
        if !self.connection_state.am_choking {
            self.connection_state.am_choking = true;
            trace!("Choking peer {}", self.peer.ip);
            self.send_message(Message::Choke).await?;
            let event: Box<dyn PeerEventHandler + Send> = Box::new(LocalChokeEventHandler {
                peer: self.peer.clone(),
            });
            if self.event_tx.send(event).is_err() {
                error!(peer.ip = %self.peer.ip, "Failed to send LocalChokeEvent");
            }
        }
        Ok(())
    }

    /// Set choking state to false and send Unchoke message.
    /// Called externally (e.g., by Swarm) based on choking algorithm.
    // Make crate-visible so SwarmCommandHandler can call it
    pub(crate) async fn set_unchoking(&mut self) -> Result<()> {
        if self.connection_state.am_choking {
            self.connection_state.am_choking = false;
            trace!("Unchoking peer {}", self.peer.ip);
            self.send_message(Message::Unchoke).await?;
            let event: Box<dyn PeerEventHandler + Send> = Box::new(LocalUnchokeEventHandler {
                peer: self.peer.clone(),
            });
            if self.event_tx.send(event).is_err() {
                error!(peer.ip = %self.peer.ip, "Failed to send LocalUnchokeEvent");
            }
        }
        Ok(())
    }

    /// Updates the local `have_bitfield`. This should be called AFTER a piece is verified.
    /// Returns true if the bitfield was actually changed.
    pub fn set_piece_as_completed(&mut self, piece_index: usize) -> bool {
        if let Some(have_bf) = self.have_bitfield.as_mut() {
            if piece_index < have_bf.len() {
                if !have_bf.get(piece_index).unwrap_or(false) {
                    // Check if already set
                    have_bf.set(piece_index, true);
                    trace!(piece_index, "Updated local have_bitfield");
                    return true; // State changed
                }
            } else {
                warn!(
                    piece_index,
                    bitfield_len = have_bf.len(),
                    "Attempted to set bitfield index out of bounds in set_piece_as_completed"
                );
            }
        } else {
            warn!(
                piece_index,
                "Have bitfield not initialized when trying to set piece as complete"
            );
        }
        false // State did not change
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_from_be_bytes_ok() {
        let raw = [192, 168, 0, 1, 0x1F, 0x90]; // 192.168.0.1:8080
        let peer = Peer::from_be_bytes(&raw).unwrap();
        assert_eq!(peer.ip, "192.168.0.1");
        assert_eq!(peer.port, 8080);
        assert!(peer.peer_id.is_none());
    }

    #[test]
    fn test_from_be_bytes_err() {
        let raw = [1, 2, 3];
        assert!(Peer::from_be_bytes(&raw).is_err());
    }

    #[test]
    fn test_from_socket_address() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 6881);
        let peer = Peer::from_socket_address(addr);
        assert_eq!(peer.ip, "10.0.0.1");
        assert_eq!(peer.port, 6881);
    }

    #[test]
    fn test_eq_and_hash() {
        use std::collections::HashSet;
        let p1 = Peer {
            peer_id: None,
            ip: "1.1.1.1".into(),
            port: 1234,
        };
        let p2 = Peer {
            peer_id: Some("id".into()),
            ip: "1.1.1.1".into(),
            port: 1234,
        };
        assert_eq!(p1, p2);
        let mut set = HashSet::new();
        set.insert(p1);
        assert!(set.contains(&p2));
    }
}
