use anyhow::{ensure, Result};
use bendy::decoding::{FromBencode, Object, ResultExt};

#[derive(Debug)]
pub struct Peer {
    pub peer_id: Option<String>,
    pub ip: String,
    pub port: u16,
}

impl Peer {
    pub fn new(peer_id: String, ip: String, port: u16) -> Self {
        Self {
            peer_id: Some(peer_id),
            ip,
            port,
        }
    }

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
