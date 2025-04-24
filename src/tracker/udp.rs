use anyhow::{bail, Context, Result};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;

use super::{Trackable, MAX_PEERS};
use crate::tracker::TrackerResponse;

// Implemented according to Bep_15 <https://www.bittorrent.org/beps/bep_0015.html>
#[derive(Debug)]
pub struct Udp {
    info_hash: Arc<Vec<u8>>,
    peer_id: String,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    size: u64,
    connection_id: u64,
    socket: UdpSocket,
    port: u16,

    // State for event reporting
    started_sent: bool,
    needs_complete_event: bool,
}

impl Udp {
    const PROTOCOL_ID: u64 = 0x41727101980;
    const MAX_RESPONSE_SIZE: usize = 20 + 6 * MAX_PEERS;

    #[instrument(skip(info_hash, peer_id), fields(url = %url, port = port, size = size))]
    pub fn new(
        url: String,
        info_hash: Arc<Vec<u8>>,
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Result<Self> {
        debug!("Creating new UDP tracker instance");
        let socket_addr_str = match Self::parse_url_to_string(&url) {
            Ok(addr_str) => addr_str,
            Err(e) => {
                error!(error = %e, "Failed to parse tracker URL");
                bail!("Failed to parse tracker URL: {}", e)
            }
        };
        // Bind socket first
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to bind UDP socket");
                bail!("Failed to bind UDP socket: {}", e)
            }
        };
        trace!(local_addr = ?socket.local_addr().ok(), "UDP socket bound");

        // Resolve address and connect socket
        let address = match socket_addr_str.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => bail!("Could not resolve tracker address: {}", socket_addr_str),
        };
        socket.connect(address)?;
        debug!(tracker_addr = %address, "UDP socket connected to tracker");

        let connection_id = match Self::connect(&socket) {
            Ok(cid) => cid,
            Err(e) => {
                error!(error = %e, "UDP tracker connection failed");
                bail!("UDP tracker connection failed: {}", e)
            }
        };
        debug!(connection_id, "UDP tracker connection established");

        Ok(Self {
            info_hash,
            peer_id,
            uploaded: 0,
            downloaded: 0,
            left: size,
            size,
            connection_id,
            socket,
            port,
            started_sent: false,
            needs_complete_event: false,
        })
    }

    // Helper to parse URL and return String to avoid lifetime issues with ToSocketAddrs
    fn parse_url_to_string(url: &str) -> Result<String> {
        let parsed_url = Url::parse(url)?;
        let host = parsed_url.host_str().context("Url has no host string")?;
        let port = parsed_url.port().context("Url has no port")?;
        Ok(format!("{}:{}", host, port))
    }

    #[instrument(skip(socket), fields(peer = ?socket.peer_addr().ok()))]
    fn connect(socket: &UdpSocket) -> Result<u64> {
        trace!("Attempting UDP connect action");
        let transaction_id: u32 = rand::random();
        debug!(tx_id = transaction_id, "Generated connect transaction ID");

        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&Self::PROTOCOL_ID.to_be_bytes());
        buf[8..12].copy_from_slice(&0u32.to_be_bytes()); // Action = 0 for connect
        buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());

        trace!(payload = ?buf, "Sending connect request");
        socket.send(&buf)?;

        let mut response_buf = [0u8; 16];
        trace!("Waiting for connect response");
        socket.recv(&mut response_buf)?;
        trace!(response = ?response_buf, "Received connect response bytes");

        let action = u32::from_be_bytes(response_buf[0..4].try_into()?);
        let received_transaction_id = u32::from_be_bytes(response_buf[4..8].try_into()?);
        debug!(
            response_action = action,
            response_tx_id = received_transaction_id,
            "Parsed connect response header"
        );

        if action != 0 {
            error!(
                expected_action = 0,
                received_action = action,
                "Received incorrect action in connect response"
            );
            bail!(
                "Response action is not connect (expected 0, got {})",
                action
            );
        }
        if transaction_id != received_transaction_id {
            error!(
                expected_tx_id = transaction_id,
                received_tx_id = received_transaction_id,
                "Transaction ID mismatch in connect response"
            );
            bail!("Transaction ID mismatch");
        }

        let connection_id = u64::from_be_bytes(response_buf[8..16].try_into()?);
        debug!(connection_id, "Successfully obtained connection ID");
        Ok(connection_id)
    }
}

impl Trackable for Udp {
    #[instrument(name="udp_scrape", skip(self), fields(connection_id = self.connection_id))]
    fn scrape(&mut self) -> Result<TrackerResponse> {
        trace!("Starting UDP scrape (announce)");
        let event_code: u32 = if !self.started_sent {
            2 // started
        } else if self.needs_complete_event {
            1 // completed
        } else {
            0 // none
        };
        let transaction_id: u32 = rand::random();
        debug!(
            event = event_code,
            tx_id = transaction_id,
            downloaded = self.downloaded,
            uploaded = self.uploaded,
            left = self.left,
            "Determined announce parameters"
        );

        let mut buf = [0u8; 98];
        buf[0..8].copy_from_slice(&self.connection_id.to_be_bytes());
        buf[8..12].copy_from_slice(&1u32.to_be_bytes()); // Action = 1 for announce
        buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());
        buf[16..36].copy_from_slice(&self.info_hash);
        buf[36..56].copy_from_slice(self.peer_id.as_bytes());
        buf[56..64].copy_from_slice(&self.downloaded.to_be_bytes());
        buf[64..72].copy_from_slice(&self.left.to_be_bytes());
        buf[72..80].copy_from_slice(&self.uploaded.to_be_bytes());
        buf[80..84].copy_from_slice(&event_code.to_be_bytes());
        buf[84..88].copy_from_slice(&0u32.to_be_bytes()); // IP address = 0
        buf[88..92].copy_from_slice(&0u32.to_be_bytes()); // Key = 0
        buf[92..96].copy_from_slice(&(-(1i32) as u32).to_be_bytes()); // Num want = -1
        buf[96..98].copy_from_slice(&self.port.to_be_bytes());

        trace!(payload_len = buf.len(), "Sending announce request");
        self.socket.send(&buf)?;

        let mut response_buf = [0u8; Self::MAX_RESPONSE_SIZE];
        trace!("Waiting for announce response");
        let received_len = self.socket.recv(&mut response_buf)?;
        trace!(
            response_len = received_len,
            "Received announce response bytes"
        );
        // Ensure we only process the bytes actually received
        let response_slice = &response_buf[..received_len];

        // Parse the response using the function from tracker.rs
        let tracker_response =
            match TrackerResponse::from_udp_response(response_slice, transaction_id) {
                Ok(resp) => resp,
                Err(e) => {
                    error!(error = %e, "Failed to parse UDP tracker response");
                    // Optionally log raw response bytes (truncated)
                    // trace!(raw_response = ?response_slice);
                    bail!("Failed to parse UDP tracker response: {}", e);
                }
            };
        trace!(?tracker_response, "Parsed UDP announce response");

        // Update state AFTER successful scrape/announce
        if event_code != 0 {
            if event_code == 2 {
                // started
                trace!("Marking 'started' event as sent");
                self.started_sent = true;
            }
            if event_code == 1 {
                // completed
                trace!("Resetting 'needs_complete_event' flag");
                self.needs_complete_event = false;
            }
        }
        debug!(
            peers_count = tracker_response.peers.len(),
            interval = tracker_response.interval,
            "Scrape successful"
        );
        Ok(tracker_response)
    }

    #[instrument(level = "trace", skip(self))]
    fn update_progress(&mut self, downloaded: u64, uploaded: u64, left: u64) {
        if left == 0 && self.left != 0 {
            debug!("Download completed, scheduling 'completed' event");
            self.needs_complete_event = true;
        }
        self.downloaded = downloaded;
        self.uploaded = uploaded;
        self.left = left;
        trace!(downloaded, uploaded, left, "Updated tracker progress");
    }
}
