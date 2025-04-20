mod message;

use std::collections::HashMap;
use std::{hash::Hash, net::SocketAddr};

use crate::client::PendingPeer;
use crate::file::Piece;
use anyhow::{ensure, Context, Result};
use async_channel::Sender;
use bendy::decoding::FromBencode;
use bit_vec::BitVec;
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{UnboundedSender, UnboundedReceiver},
};
use crate::swarm::{PeerEvent, SwarmCommand};

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
    piece_hashes: Vec<[u8; 20]>,
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
        info_hash: &'static [u8],
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Vec<[u8; 20]>,
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
        info_hash: &'static [u8],
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Vec<[u8; 20]>,
        event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
    ) -> Result<Self> {
        let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .context("Connect to TcpStream")?;
        Self::write_handshake(&mut stream, info_hash, &my_id).await?;
        Self::read_handshake(&mut stream, info_hash).await?;
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
        info_hash: &'static [u8],
        piece_sender: Sender<Piece>,
        piece_length: usize,
        piece_hashes: Vec<[u8; 20]>,
        event_tx: UnboundedSender<PeerEvent>,
        cmd_rx: UnboundedReceiver<SwarmCommand>,
    ) -> Result<Self> {
        Self::read_handshake(&mut stream, info_hash).await?;
        Self::write_handshake(&mut stream, info_hash, &my_id).await?;
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

    pub async fn start(&mut self) {
        // send our bitfield then express interest
        if let Some(ref have) = self.have_bitfield {
            let _ = self
                .send_message(message::Message::Bitfield(have.clone()))
                .await;
        }
        self.send_message(message::Message::Interested)
            .await
            .expect("Failed to send Interested");

        let mut len_buf = [0u8; 4];
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    let SwarmCommand::Request(piece, begin, length) = cmd;
                    let _ = self.send_message(message::Message::Request(piece, begin, length)).await;
                }
                read = self.stream.read_exact(&mut len_buf) => {
                    if read.is_err() { break; }
                    let len = u32::from_be_bytes(len_buf);
                    if len == 0 { continue; }
                    let mut msg_buf = vec![0u8; len as usize];
                    if self.stream.read_exact(&mut msg_buf).await.is_err() { break; }
                    if let Ok(msg) = message::Message::from_be_bytes(&msg_buf) {
                        match msg {
                            message::Message::Bitfield(bf) => {
                                self.peer_bitfield = Some(bf.clone());
                                if self.have_bitfield.is_none() {
                                    self.have_bitfield = Some(BitVec::from_elem(bf.len(), false));
                                }
                                let _ = self.event_tx.send(PeerEvent::Bitfield(self.peer.clone(), bf.clone()));
                            }
                            message::Message::Have(idx) => {
                                if let Some(ref mut bf) = self.peer_bitfield {
                                    bf.set(idx as usize, true);
                                }
                                let _ = self.event_tx.send(PeerEvent::Have(self.peer.clone(), idx as usize));
                            }
                            message::Message::Unchoke => {
                                self.peer_choking = false;
                                let _ = self.event_tx.send(PeerEvent::Unchoke(self.peer.clone()));
                            }
                            message::Message::Choke => {
                                self.peer_choking = true;
                                let _ = self.event_tx.send(PeerEvent::Choke(self.peer.clone()));
                            }
                            message::Message::Piece(piece_index, begin, block) => {
                                println!(
                                    "Received piece {} ({} bytes) from {}",
                                    piece_index,
                                    block.len(),
                                    self.peer.ip
                                );
                                if let Some(ref mut have) = self.have_bitfield {
                                    have.set(piece_index as usize, true);
                                }
                                let _ = self.send_message(message::Message::Have(piece_index)).await;
                                // assemble piece blocks
                                let blocks = self
                                    .pending_pieces
                                    .entry(piece_index as usize)
                                    .or_insert_with(|| vec![None; self.piece_length.div_ceil(16384)]);
                                let idx = begin as usize / 16384;
                                blocks[idx] = Some(block.clone());
                                // check if complete
                                if blocks.iter().all(Option::is_some) {
                                    let mut data = Vec::with_capacity(self.piece_length);
                                    for b in blocks.iter() {
                                        data.extend(b.as_ref().unwrap());
                                    }
                                    // verify hash
                                    let mut hasher = Sha1::new();
                                    hasher.update(&data);
                                    if hasher.finalize().as_slice()
                                        == self.piece_hashes[piece_index as usize]
                                    {
                                        self.piece_sender
                                            .send(Piece {
                                                index: piece_index,
                                                data,
                                            })
                                            .await
                                            .expect("Failed to send piece");
                                    } else {
                                        println!("Piece {} failed hash check", piece_index);
                                    }
                                    self.pending_pieces.remove(&(piece_index as usize));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

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

    async fn send_message(&mut self, message: message::Message) -> Result<()> {
        let bytes = message.to_be_bytes();
        self.stream.write_all(&bytes).await?;
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
