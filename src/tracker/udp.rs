use anyhow::{bail, Context, Result};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use url::Url;

use super::{Trackable, MAX_PEERS};
use crate::tracker::TrackerResponse;

// Implemented according to Bep_15 <https://www.bittorrent.org/beps/bep_0015.html>
pub struct Udp {
    info_hash: [u8; 20],
    peer_id: String,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    connection_id: u64,
    socket: UdpSocket,
    address: SocketAddr,
    port: u16,
}

impl Udp {
    const PROTOCOL_ID: u64 = 0x41727101980;
    const MAX_RESPONSE_SIZE: usize = 20 + 6 * MAX_PEERS;

    pub fn new(url: String, info_hash: [u8; 20], peer_id: String, port: u16) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        let address = Self::parse_url(&url)?;

        // Make connection request
        socket.connect(address)?;
        let connection_id = Self::connect(&socket)?;

        Ok(Self {
            info_hash,
            peer_id,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            connection_id,
            socket,
            address,
            port,
        })
    }

    fn parse_url(url: &str) -> Result<SocketAddr> {
        let url = Url::parse(url)?;
        format!(
            "{}:{}",
            url.host_str().context("Url has no host string")?,
            url.port().context("Url has no port")?
        )
        .to_socket_addrs()?
        .next()
        .context("Could not resolve url")
    }

    fn connect(socket: &UdpSocket) -> Result<u64> {
        let transaction_id: u32 = rand::random();

        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&Self::PROTOCOL_ID.to_be_bytes());
        buf[8..12].copy_from_slice(&0u32.to_be_bytes()); // Action = 0 for connect
        buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());

        socket.send(&buf)?;

        let mut buf = [0u8; 16];
        socket.recv(&mut buf)?;

        let recieved_transaction_id = u32::from_be_bytes(buf[4..8].try_into()?);
        if transaction_id != recieved_transaction_id {
            bail!("Transaction ID mismatch");
        }

        let action = u32::from_be_bytes(buf[0..4].try_into()?);
        if action != 0 {
            bail!("Response action is not connect");
        }

        Ok(u64::from_be_bytes(buf[8..16].try_into()?))
    }
}

impl Trackable for Udp {
    fn scrape(&mut self) -> Result<TrackerResponse> {
        let transaction_id: u32 = rand::random();

        let mut buf = [0u8; 98];
        buf[0..8].copy_from_slice(&self.connection_id.to_be_bytes());
        buf[8..12].copy_from_slice(&1u32.to_be_bytes()); // Action = 1 for announce
        buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());
        buf[16..36].copy_from_slice(&self.info_hash);
        buf[36..56].copy_from_slice(&self.peer_id.as_bytes());
        buf[56..64].copy_from_slice(&self.downloaded.to_be_bytes());
        buf[64..72].copy_from_slice(&self.left.to_be_bytes());
        buf[72..80].copy_from_slice(&self.uploaded.to_be_bytes());
        buf[80..84].copy_from_slice(&0u32.to_be_bytes()); // Event = 0 for none (for now)
        buf[84..88].copy_from_slice(&0u32.to_be_bytes()); // IP address = 0 for default
        buf[88..92].copy_from_slice(&0u32.to_be_bytes()); // Key = 0 for default
        buf[92..96].copy_from_slice(&MAX_PEERS.to_be_bytes()); // Num want = -1 for default
        buf[96..98].copy_from_slice(&self.port.to_be_bytes());

        self.socket.send(&buf)?;

        let mut buf = [0u8; Self::MAX_RESPONSE_SIZE];
        self.socket.recv(&mut buf)?;

        Ok(TrackerResponse::from_udp_response(&buf)?)
    }
}
