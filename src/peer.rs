mod message;

use std::{hash::Hash, net::SocketAddr};

use anyhow::{ensure, Context, Result};
use bendy::decoding::{FromBencode, ResultExt as _};
use bit_vec::BitVec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::client::PendingPeer;

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
                (b"peer id", value) => {
                    peer_id = String::decode_bencode_object(value)
                        .context("peer id")
                        .map(Some)?
                }
                (b"ip", value) => {
                    ip = String::decode_bencode_object(value)
                        .context("ip")
                        .map(Some)?
                }
                (b"port", value) => {
                    port = u16::decode_bencode_object(value)
                        .context("port")
                        .map(Some)?
                }
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
    my_id: String,
    stream: TcpStream,
    bitfield: Option<BitVec>,

    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
}

impl PeerConnection {
    const PSTR: &'static str = "BitTorrent protocol";

    pub async fn start(&mut self) {
        // After handshake, express interest to receive bitfield
        self.send_message(message::Message::Interested).await.expect("Failed to send Interested");
        let mut len_buf = [0u8; 4];
        loop {
            // Read message length prefix
            if let Err(_) = self.stream.read_exact(&mut len_buf).await {
                break;
            }
            let len = u32::from_be_bytes(len_buf);
            if len == 0 {
                // keep-alive, no payload
                continue;
            }
            // Read the rest of the message
            let mut msg_buf = vec![0u8; len as usize];
            if let Err(_) = self.stream.read_exact(&mut msg_buf).await {
                break;
            }
            // Parse and handle message
            if let Ok(msg) = message::Message::from_be_bytes(&msg_buf) {
                match msg {
                    message::Message::Bitfield(bitfield) => {
                        println!("Received bitfield from {}: {} bits", self.peer.ip, bitfield.len());
                        self.bitfield = Some(bitfield.clone());
                        // Trigger requesting after initial bitfield
                        self.request_rarest_piece().await;
                    }
                    message::Message::Have(index) => {
                        println!("Peer {} has new piece {}", self.peer.ip, index);
                        if let Some(ref mut bf) = self.bitfield {
                            bf.set(index as usize, true);
                        }
                    }
                    message::Message::Unchoke => {
                        println!("Peer {} unchoked us", self.peer.ip);
                        // Can request more blocks now
                        self.request_rarest_piece().await;
                    }
                    _ => {
                        // For now ignore other message types
                    }
                }
            }
        }
    }

    // Stub: selects a piece from the stored bitfield and sends a request
    async fn request_rarest_piece(&mut self) {
        // TODO: tally availability across all peers, sort by rarity, and request blocks for the rarest one.
        if let Some(ref bf) = self.bitfield {
            // For now, simply pick the first available piece
            if let Some(idx) = bf.iter().position(|b| b) {
                let block_size = 16 * 1024;
                let begin = 0;
                let length = block_size as u32;
                println!("Requesting piece {} ({} bytes) from {}", idx, length, self.peer.ip);
                let _ = self
                    .send_message(message::Message::Request(idx as u32, begin, length))
                    .await;
            }
        }
    }

    pub async fn new(peer: PendingPeer, my_id: String, info_hash: &'static [u8]) -> Result<Self> {
        Ok(match peer {
            PendingPeer::Outgoing(peer) => Self::new_outgoing(peer, my_id, info_hash).await?,
            PendingPeer::Incoming(peer, stream) => {
                Self::new_incoming(peer, stream, my_id, info_hash).await?
            }
        })
    }

    async fn new_outgoing(peer: Peer, my_id: String, info_hash: &'static [u8]) -> Result<Self> {
        let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .await
            .context("Connect to TcpStream")?;
        Self::write_handshake(&mut stream, info_hash, &my_id).await?;
        Self::read_handshake(&mut stream, info_hash).await?;
        Ok(Self {
            peer,
            my_id,
            stream,
            bitfield: None,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        })
    }

    async fn new_incoming(
        peer: Peer,
        mut stream: TcpStream,
        my_id: String,
        info_hash: &'static [u8],
    ) -> Result<Self> {
        Self::read_handshake(&mut stream, info_hash).await?;
        Self::write_handshake(&mut stream, info_hash, &my_id).await?;
        Ok(Self {
            peer,
            my_id,
            stream,
            bitfield: None,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        })
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
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10,0,0,1)), 6881);
        let peer = Peer::from_socket_address(addr);
        assert_eq!(peer.ip, "10.0.0.1");
        assert_eq!(peer.port, 6881);
    }

    #[test]
    fn test_eq_and_hash() {
        use std::collections::HashSet;
        let p1 = Peer { peer_id: None, ip: "1.1.1.1".into(), port: 1234 };
        let p2 = Peer { peer_id: Some("id".into()), ip: "1.1.1.1".into(), port: 1234 };
        assert_eq!(p1, p2);
        let mut set = HashSet::new();
        set.insert(p1);
        assert!(set.contains(&p2));
    }
}
