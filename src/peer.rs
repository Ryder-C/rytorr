mod message;

use std::{hash::Hash, net::TcpStream, sync::Arc};

use anyhow::{ensure, Context, Result};
use bit_vec::BitVec;
use tokio::sync::Mutex;

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

pub struct PeerConnection {
    peer: Peer,
    stream: TcpStream,
    bitfield: Option<BitVec>,

    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
}

impl PeerConnection {
    pub fn new(peer: Peer) -> Result<Self> {
        let stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port))
            .context("Connect to TcpStream")?;
        Ok(Self {
            peer,
            stream,
            bitfield: None,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        })
    }
}
