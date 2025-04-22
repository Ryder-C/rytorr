mod message;

use std::collections::HashMap;
use std::{hash::Hash, net::SocketAddr, sync::Arc};

use crate::engine::PendingPeer;
use crate::file::Piece;
use crate::swarm::{PeerEvent, SwarmCommand};
use anyhow::{ensure, Context, Result};
use async_channel::Sender;
use bendy::decoding::FromBencode;
use bit_vec::BitVec;
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{debug, error, info, trace, warn};

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

    event_tx: UnboundedSender<PeerEvent>,
    cmd_rx: UnboundedReceiver<SwarmCommand>,
}

impl PeerConnection {
    const PSTR: &'static str = "BitTorrent protocol";

    pub async fn new(
        peer: PendingPeer,
        my_id: String,
        info_hash: Arc<Vec<u8>>,
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Arc<Vec<[u8; 20]>>,
        event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
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
        event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
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
        event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
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
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        SwarmCommand::Request(piece, begin, length) => {
                            debug!(piece_index = piece, begin, length, "Received Request command from swarm");
                            if let Err(e) = self.send_message(message::Message::Request(piece, begin, length)).await {
                                error!(error = %e, "Failed to send Request message");
                                // Consider breaking the loop or notifying swarm of failure
                            }
                        }
                    }
                }
                read_result = self.stream.read_exact(&mut len_buf) => {
                    match read_result {
                        Ok(_) => {
                            let len = u32::from_be_bytes(len_buf);
                            trace!(message_len = len, "Received message length");
                            if len == 0 {
                                debug!("Received KeepAlive");
                                continue; // KeepAlive, just continue loop
                            }
                            let mut msg_buf = vec![0u8; len as usize];
                            if let Err(e) = self.stream.read_exact(&mut msg_buf).await {
                                error!(error = %e, "Failed to read message body");
                                break; // Assume connection is broken
                            }
                            match message::Message::from_be_bytes(&msg_buf) {
                                Ok(msg) => self.handle_message(msg).await,
                                Err(e) => {
                                    error!(error = %e, "Failed to parse message");
                                    // Decide whether to break or continue
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

    #[tracing::instrument(skip(self, msg), fields(peer.ip = %self.peer.ip))]
    async fn handle_message(&mut self, msg: message::Message) {
        match msg {
            message::Message::KeepAlive => trace!("Received KeepAlive"),
            message::Message::Choke => {
                debug!("Received Choke");
                self.peer_choking = true;
                let _ = self.event_tx.send(PeerEvent::Choke(self.peer.clone()));
            }
            message::Message::Unchoke => {
                debug!("Received Unchoke");
                self.peer_choking = false;
                let _ = self.event_tx.send(PeerEvent::Unchoke(self.peer.clone()));
            }
            message::Message::Interested => {
                debug!("Received Interested");
                self.peer_interested = true;
                // TODO: Implement unchoking logic (send Unchoke if we want to serve this peer)
                // if !self.am_choking { ... send Unchoke ... }
            }
            message::Message::NotInterested => {
                debug!("Received NotInterested");
                self.peer_interested = false;
            }
            message::Message::Have(idx) => {
                debug!(piece_index = idx, "Received Have");
                if let Some(ref mut bf) = self.peer_bitfield {
                    if idx < bf.len() as u32 {
                        // Check bounds
                        bf.set(idx as usize, true);
                    } else {
                        warn!(
                            piece_index = idx,
                            bitfield_len = bf.len(),
                            "Received Have for index out of bounds"
                        );
                    }
                } else {
                    // We should ideally wait for Bitfield first, but can handle Have anyway
                    warn!("Received Have before Bitfield");
                }
                let _ = self
                    .event_tx
                    .send(PeerEvent::Have(self.peer.clone(), idx as usize));
            }
            message::Message::Bitfield(bf) => {
                debug!(bitfield_len = bf.len(), "Received Bitfield");
                if self.peer_bitfield.is_some() {
                    warn!("Received Bitfield message twice");
                    // Maybe disconnect peer? For now, just overwrite.
                }
                // TODO: Verify bitfield length against number of pieces from torrent info
                self.peer_bitfield = Some(bf.clone());
                let _ = self
                    .event_tx
                    .send(PeerEvent::Bitfield(self.peer.clone(), bf));
            }
            message::Message::Request(idx, begin, len) => {
                debug!(piece_index = idx, begin, len, "Received Request");
                // TODO: Implement logic to read from file and send Piece message
                warn!("Piece request handling not implemented");
            }
            message::Message::Piece(piece_index, begin, block) => {
                // Use debug level for block receipt, info for completion/verification
                debug!(
                    piece_index,
                    begin,
                    block_len = block.len(),
                    "Received Piece block"
                );
                // TODO: Need a more robust way to track requested blocks/pieces
                // to verify this piece is one we actually asked for.
                // assemble piece blocks
                let num_blocks = self.piece_length.div_ceil(16384); // Assuming 16 KiB blocks
                let blocks = self
                    .pending_pieces
                    .entry(piece_index as usize)
                    .or_insert_with(|| vec![None; num_blocks]);
                let block_index = begin as usize / 16384;
                if block_index >= blocks.len() {
                    error!(
                        piece_index,
                        begin,
                        block_len = block.len(),
                        num_blocks,
                        "Received block with invalid begin offset"
                    );
                    return;
                }
                if blocks[block_index].is_some() {
                    warn!(piece_index, block_index, "Received duplicate block");
                    // Potentially ignore or handle as error
                }
                blocks[block_index] = Some(block);
                // check if complete
                if blocks.iter().all(Option::is_some) {
                    trace!(piece_index, "All blocks received, assembling piece");
                    let mut data = Vec::with_capacity(self.piece_length);
                    for b_opt in blocks.iter() {
                        // Use unwrap_unchecked assuming all are Some due to all() check
                        data.extend(unsafe { b_opt.as_ref().unwrap_unchecked() });
                    }
                    // TODO: Adjust data length if it's the last piece and shorter than piece_length
                    // verify hash
                    trace!(piece_index, "Verifying piece hash");
                    let mut hasher = Sha1::new();
                    hasher.update(&data);
                    let calculated_hash = hasher.finalize();
                    let expected_hash = &self.piece_hashes[piece_index as usize];
                    if calculated_hash.as_slice() == expected_hash {
                        // Use info level for successful verification
                        info!(
                            piece_index,
                            data_len = data.len(),
                            "Piece completed and verified"
                        );
                        // Send to swarm/disk writer
                        if let Err(e) = self
                            .piece_sender
                            .send(Piece {
                                index: piece_index,
                                data,
                            })
                            .await
                        {
                            error!(error = %e, piece_index, "Failed to send completed piece to swarm");
                            // If channel is closed, maybe we should stop the connection?
                        }
                        // TODO: Send Have message? Update local bitfield?
                    } else {
                        // Use error level for failed verification
                        error!(piece_index, expected_hash = ?expected_hash, calculated_hash = ?calculated_hash.as_slice(), "Piece failed hash check!");
                        // TODO: Discard piece data, re-request blocks, potentially penalize peer
                    }
                    // Remove from pending pieces regardless of verification result
                    self.pending_pieces.remove(&(piece_index as usize));
                }
            }
            message::Message::Cancel(idx, begin, len) => {
                debug!(piece_index = idx, begin, len, "Received Cancel");
                // TODO: Implement cancellation logic if supporting pipelined requests
                warn!("Cancel handling not implemented");
            }
            message::Message::Port(port) => {
                debug!(dht_port = port, "Received Port message (DHT)");
                // TODO: Implement DHT integration if needed
                warn!("Port message handling not implemented");
            }
        }
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

    #[tracing::instrument(skip(self, message), fields(message_type = std::any::type_name::<message::Message>()))]
    async fn send_message(&mut self, message: message::Message) -> Result<()> {
        let payload = message.to_be_bytes();
        self.stream.write_all(&payload).await?;
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
