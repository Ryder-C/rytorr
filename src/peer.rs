mod handlers;
pub(crate) mod message;

use std::collections::HashMap;
use std::time::Instant;
use std::{hash::Hash, net::SocketAddr, sync::Arc};

use crate::engine::PendingPeer;
use crate::file::Piece;
use crate::swarm::PeerEventHandler;
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

pub struct PeerConnection {
    pub peer: Peer,
    pub piece_sender: Sender<Piece>,
    piece_length: usize,
    piece_hashes: Arc<Vec<[u8; 20]>>,
    pending_pieces: HashMap<usize, Vec<Option<Vec<u8>>>>,
    my_id: String,
    stream: TcpStream,
    pub peer_bitfield: Option<BitVec>,
    pub have_bitfield: Option<BitVec>,

    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,

    event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
    cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,

    // Shared state from Swarm
    uploaded_counter: Arc<RwLock<u64>>,
    read_file_handle: Arc<Mutex<File>>,
    piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
    pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
}

// --- PeerConnection Implementation ---
impl PeerConnection {
    const PSTR: &'static str = "BitTorrent protocol";

    pub async fn new(
        peer: PendingPeer,
        my_id: String,
        info_hash: Arc<Vec<u8>>,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        uploaded_counter: Arc<RwLock<u64>>,
        read_file_handle: Arc<Mutex<File>>,
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) -> Result<Self> {
        let conn = match peer {
            PendingPeer::Outgoing(p) => {
                Self::new_outgoing(
                    p,
                    my_id,
                    info_hash,
                    piece_sender.clone(),
                    piece_length,
                    piece_hashes.clone(),
                    uploaded_counter.clone(),
                    read_file_handle.clone(),
                    event_tx.clone(),
                    cmd_rx,
                    piece_download_progress.clone(),
                    pending_requests.clone(),
                )
                .await?
            }
            PendingPeer::Incoming(p, s) => {
                Self::new_incoming(
                    p,
                    s,
                    my_id,
                    info_hash,
                    piece_sender.clone(),
                    piece_length,
                    piece_hashes.clone(),
                    uploaded_counter.clone(),
                    read_file_handle.clone(),
                    event_tx.clone(),
                    cmd_rx,
                    piece_download_progress.clone(),
                    pending_requests.clone(),
                )
                .await?
            }
        };
        // This init seems redundant now, should be handled by Swarm
        // conn.pending_pieces = HashMap::new();
        Ok(conn)
    }

    async fn new_outgoing(
        peer: Peer,
        my_id: String,
        info_hash: Arc<Vec<u8>>,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        uploaded_counter: Arc<RwLock<u64>>,
        read_file_handle: Arc<Mutex<File>>,
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .context("Connect to TcpStream")?;
        Self::write_handshake(&mut stream, &info_hash, &my_id).await?;
        Self::read_handshake(&mut stream, &info_hash).await?;
        let have = BitVec::from_elem(piece_hashes.len(), false);
        Ok(Self {
            peer,
            piece_sender,
            piece_length,
            piece_hashes,
            pending_pieces: HashMap::new(),
            my_id,
            stream,
            peer_bitfield: None,
            have_bitfield: Some(have),
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            event_tx,
            cmd_rx,
            uploaded_counter,
            read_file_handle,
            piece_download_progress,
            pending_requests,
        })
    }

    async fn new_incoming(
        peer: Peer,
        mut stream: TcpStream,
        my_id: String,
        info_hash: Arc<Vec<u8>>,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        uploaded_counter: Arc<RwLock<u64>>,
        read_file_handle: Arc<Mutex<File>>,
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
        piece_download_progress: Arc<Mutex<HashMap<usize, u32>>>,
        pending_requests: Arc<Mutex<HashMap<(usize, u32), Instant>>>,
    ) -> Result<Self> {
        Self::read_handshake(&mut stream, &info_hash).await?;
        Self::write_handshake(&mut stream, &info_hash, &my_id).await?;
        let have = BitVec::from_elem(piece_hashes.len(), false);
        Ok(Self {
            peer,
            piece_sender,
            piece_length,
            piece_hashes,
            pending_pieces: HashMap::new(),
            my_id,
            stream,
            peer_bitfield: None,
            have_bitfield: Some(have),
            am_choking: false,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            event_tx,
            cmd_rx,
            uploaded_counter,
            read_file_handle,
            piece_download_progress,
            pending_requests,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn start(&mut self) {
        // send our bitfield then express interest
        if let Some(ref have) = self.have_bitfield {
            debug!("Sending initial Bitfield");
            if let Err(e) = self
                .send_message(message::Message::Bitfield(have.clone()))
                .await
            {
                error!(error = %e, "Failed to send initial Bitfield");
                return; // Can't proceed without sending bitfield
            }
        }
        debug!("Sending Interested");
        if let Err(e) = self.send_message(message::Message::Interested).await {
            error!(error = %e, "Failed to send Interested");
            return; // Can't proceed if we can't express interest
        }

        let mut len_buf = [0u8; 4];
        loop {
            trace!("Peer loop waiting for command or message");
            tokio::select! {
                // Biased select ensures we prioritize sending commands over reading messages if both are ready
                // This might be desirable for responsiveness to requests
                biased;
                Some(cmd_handler) = self.cmd_rx.recv() => {
                    // Directly handle the command using the trait object
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

    #[tracing::instrument(skip(self, message), fields(message_type = std::any::type_name::<message::Message>()))]
    pub(crate) async fn send_message(&mut self, message: message::Message) -> Result<()> {
        let payload = message.to_be_bytes();
        self.stream.write_all(&payload).await?;
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
    async fn update_interest_state(&mut self) -> Result<()> {
        let should_be_interested = self.should_i_be_interested();

        if !self.am_interested && should_be_interested {
            self.am_interested = true;
            trace!("Becoming interested in peer {}", self.peer.ip);
            self.send_message(Message::Interested).await?;
        } else if self.am_interested && !should_be_interested {
            self.am_interested = false;
            trace!("Becoming not interested in peer {}", self.peer.ip);
            self.send_message(Message::NotInterested).await?;
        }
        // No change needed if state matches calculation
        Ok(())
    }

    /// Set choking state to true and send Choke message.
    /// Called externally (e.g., by Swarm) based on choking algorithm.
    // Make crate-visible so SwarmCommandHandler can call it
    pub(crate) async fn set_choking(&mut self) -> Result<()> {
        if !self.am_choking {
            self.am_choking = true;
            trace!("Choking peer {}", self.peer.ip);
            self.send_message(Message::Choke).await?;
        }
        Ok(())
    }

    /// Set choking state to false and send Unchoke message.
    /// Called externally (e.g., by Swarm) based on choking algorithm.
    // Make crate-visible so SwarmCommandHandler can call it
    pub(crate) async fn set_unchoking(&mut self) -> Result<()> {
        if self.am_choking {
            self.am_choking = false;
            trace!("Unchoking peer {}", self.peer.ip);
            self.send_message(Message::Unchoke).await?;
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
