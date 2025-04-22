mod handlers;
mod message;

use std::collections::HashMap;
use std::{hash::Hash, net::SocketAddr, sync::Arc};

use crate::engine::PendingPeer;
use crate::file::Piece;
use crate::swarm::PeerEventHandler;
use anyhow::{ensure, Context, Result};
use async_channel::Sender;
use async_trait::async_trait;
use bendy::decoding::FromBencode;
use bit_vec::BitVec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{debug, error, info, trace};

use crate::peer::handlers::MessageHandler;
use crate::peer::message::from_bytes_to_handler;

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
}

// --- Swarm Command Handling ---

#[async_trait]
pub(super) trait SwarmCommandHandler: Send {
    async fn handle(&self, connection: &mut PeerConnection);
}

pub(crate) struct RequestCommandHandler {
    pub(crate) piece: u32,
    pub(crate) begin: u32,
    pub(crate) length: u32,
}

#[async_trait]
impl SwarmCommandHandler for RequestCommandHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(
            piece_index = self.piece,
            begin = self.begin,
            length = self.length,
            "Handling Request command"
        );
        if let Err(e) = connection
            .send_message(message::Message::Request(
                self.piece,
                self.begin,
                self.length,
            ))
            .await
        {
            error!(error = %e, "Failed to send Request message");
            // Consider breaking the loop or notifying swarm of failure
        }
    }
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
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
    ) -> Result<Self> {
        let mut conn = match peer {
            PendingPeer::Outgoing(p) => {
                Self::new_outgoing(
                    p,
                    my_id,
                    info_hash,
                    piece_sender.clone(),
                    piece_length,
                    piece_hashes.clone(),
                    event_tx.clone(),
                    cmd_rx,
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
                    event_tx.clone(),
                    cmd_rx,
                )
                .await?
            }
        };
        conn.pending_pieces = HashMap::new();
        Ok(conn)
    }

    async fn new_outgoing(
        peer: Peer,
        my_id: String,
        info_hash: Arc<Vec<u8>>,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
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
        event_tx: UnboundedSender<Box<dyn PeerEventHandler + Send>>,
        cmd_rx: UnboundedReceiver<Box<dyn SwarmCommandHandler + Send>>,
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
    async fn send_message(&mut self, message: message::Message) -> Result<()> {
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
