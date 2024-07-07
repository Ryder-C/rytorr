use std::ffi::CStr;

use anyhow::{bail, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const PSTR: &str = "BitTorrent protocol";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Peer {
    pub ip: String,
    pub port: u16,
    pub peer_id: Option<[u8; 20]>,
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
}

impl Peer {
    pub fn new(ip: String, port: u16, peer_id: Option<[u8; 20]>) -> Self {
        Self {
            ip,
            port,
            peer_id,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }

    pub fn from_be_bytes(bytes: &[u8]) -> Self {
        let ip = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);
        Self {
            ip,
            port,
            peer_id: None,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }

    pub async fn connect(
        &mut self,
        info_hash: &[u8],
        peer_id: &str,
    ) -> Result<TcpStream, Box<dyn std::error::Error>> {
        // Connect to Peer
        let mut stream = TcpStream::connect(format!("{}:{}", self.ip, self.port)).await?;
        // Send Handshake
        stream
            .write_all(&Self::build_handshake(info_hash, peer_id))
            .await?;
        stream
            .write_all(&Self::build_handshake(info_hash, peer_id))
            .await?;
        // Recieve Handshake
        let mut buf = [0u8; 49 + PSTR.len()];
        stream.read(&mut buf).await?;
        self.peer_id = Some(Self::parse_handshake(&buf, info_hash)?);
        println!("Recieved handshake from peer: {:?}", self.peer_id);

        Ok(stream)
    }

    fn parse_handshake(buf: &[u8], info_hash: &[u8]) -> Result<[u8; 20]> {
        // pstrlen
        let pstrlen = buf[0] as usize;
        if pstrlen != PSTR.len() {
            bail!("Protocol String Length Mismatch: Should be \"{}\", got \"{}\"", PSTR.len(), pstrlen);
        }
        // pstr
        let pstr = CStr::from_bytes_with_nul(&buf[1..=PSTR.len() + 1])?.to_str()?;
        if pstr != PSTR {
            bail!("Protocol String Mismatch: Should be \"{}\", got \"{}\"", PSTR, pstr);
        }
        // reserved
        // let reserved = &buf[PSTR.len() + 2..=PSTR.len() + 8];
        // info_hash
        let new_info_hash = &buf[PSTR.len() + 9..=PSTR.len() + 28];
        if new_info_hash != info_hash {
            bail!(
                "Info Hash Mismatch: Should be \"{}\", got \"{}\"",
                hex::encode(info_hash),
                hex::encode(new_info_hash)
            );
        }
        // peer_id
        let peer_id: [u8; 20] = buf[PSTR.len() + 29..=PSTR.len() + 48].try_into()?;

        Ok(peer_id)
    }

    fn build_handshake(info_hash: &[u8], peer_id: &str) -> [u8; 49 + PSTR.len()] {
        let mut buf = [0u8; 49 + PSTR.len()];


        // pstrlen
        buf[0] = PSTR.len() as u8;
        // pstr
        buf[1..=PSTR.len()].copy_from_slice(PSTR.as_bytes());
        // reserved
        buf[(PSTR.len() + 1)..=PSTR.len() + 8].copy_from_slice(&[0; 8]);
        // info_hash
        buf[(PSTR.len() + 9)..=PSTR.len() + 28].copy_from_slice(info_hash);
        // peer_id
        buf[(PSTR.len() + 28)..=PSTR.len() + 47].copy_from_slice(peer_id.as_bytes());

        buf
    }
}
