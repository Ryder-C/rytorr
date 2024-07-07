use crate::constants::MAX_PEERS;
use crate::{peer::Peer, tracking::Trackable};
use anyhow::{bail, Result};
use async_trait::async_trait;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::{
    error::Error,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
};
use url::Url;

pub struct UDP {
    pub url: String,
    pub info_hash: [u8; 20],
    pub peer_id: String,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: String,
    conn_id: Option<i64>,
}

impl UDP {
    const CONN_ID: i64 = 0x41727101980;

    pub fn new(
        url: String,
        info_hash: [u8; 20],
        peer_id: String,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: String,
    ) -> Self {
        Self {
            url,
            info_hash,
            peer_id,
            port,
            uploaded,
            downloaded,
            left,
            event,
            conn_id: None,
        }
    }

    fn build_announce_request(&self, trans_id: i32, conn_id: i64, key: i32) -> [u8; 98] {
        let event_code = if self.event == "completed" {
            1i32.to_be_bytes()
        } else if self.event == "started" {
            2i32.to_be_bytes()
        } else if self.event == "stopped" {
            3i32.to_be_bytes()
        } else {
            0i32.to_be_bytes()
        };

        let mut buf = [0; 98];
        // connection id
        buf[0..8].copy_from_slice(&conn_id.to_be_bytes());
        // action (1 for announce)
        buf[8..12].copy_from_slice(&1i32.to_be_bytes());
        // transaction id
        buf[12..16].copy_from_slice(&trans_id.to_be_bytes());
        // info hash
        buf[16..36].copy_from_slice(&self.info_hash);
        // peer_id
        buf[36..56].copy_from_slice(self.peer_id.as_bytes());
        // downloaded
        buf[56..64].copy_from_slice(&self.downloaded.to_be_bytes());
        // left
        buf[64..72].copy_from_slice(&self.left.to_be_bytes());
        // uploaded
        buf[72..80].copy_from_slice(&self.uploaded.to_be_bytes());
        // event (0 for none, 1 for completed, 2 for started, 3 for stopped)
        buf[80..84].copy_from_slice(&event_code);
        // ip address (0 for default)
        buf[84..88].copy_from_slice(&0i32.to_be_bytes());
        // key
        buf[88..92].copy_from_slice(&key.to_be_bytes());
        // num_want (-1 for default)
        buf[92..96].copy_from_slice(&(-1i32).to_be_bytes());
        // port
        buf[96..98].copy_from_slice(&self.port.to_be_bytes());

        buf
    }

    // returns (leechers, seeders, ip addresses)
    fn recv_announce_response(
        socket: &UdpSocket,
        trans_id: i32,
    ) -> Result<(u32, u32, u32, Vec<Peer>)> {
        const MAX_RESPONSE_LENGTH: usize = 20 + 6 * MAX_PEERS;
        let mut buf = [0; MAX_RESPONSE_LENGTH];
        let recieved = match socket.recv(&mut buf) {
            Ok(recieved) => recieved,
            // Will error if the response is too long
            Err(_) => MAX_RESPONSE_LENGTH,
        };

        let interval = u32::from_be_bytes(buf[8..12].try_into()?);
        let leechers = u32::from_be_bytes(buf[12..16].try_into()?);
        let seeders = u32::from_be_bytes(buf[16..20].try_into()?);

        // Check that the transaction ID matches
        let recieved_trans_id = i32::from_be_bytes(buf[4..8].try_into()?);
        if recieved_trans_id != trans_id {
            bail!(
                "Transaction ID mismatch. Expected {}, got {}",
                trans_id,
                recieved_trans_id
            );
        }
        // Check that the action is announce
        if i32::from_be_bytes(buf[0..4].try_into()?) != 1 {
            bail!("Response action is not Announce");
        }

        fn get_addresses(buf: &[u8]) -> Vec<Peer> {
            buf.chunks(6).map(Peer::from_be_bytes).collect()
        }

        Ok((
            interval,
            leechers,
            seeders,
            get_addresses(&buf[20..recieved]),
        ))
    }

    fn build_conn_request(trans_id: i32) -> [u8; 16] {
        let mut buf = [0; 16];
        // connection id
        buf[0..8].copy_from_slice(&Self::CONN_ID.to_be_bytes());
        // action (0 for connect)
        buf[8..12].copy_from_slice(&0u32.to_be_bytes());
        // transaction id
        buf[12..16].copy_from_slice(&trans_id.to_be_bytes());
        buf
    }

    // Returns given connection ID
    fn recv_conn_response(socket: &UdpSocket, trans_id: i32) -> Result<i64> {
        let mut buf = [0; 16];
        socket.recv_from(&mut buf)?;

        let recieved_trans_id = i32::from_be_bytes(buf[4..8].try_into()?);
        if trans_id != recieved_trans_id {
            bail!(
                "Transaction ID mismatch. Expected {}, got {}",
                trans_id,
                recieved_trans_id
            );
        }

        let action = i32::from_be_bytes(buf[0..4].try_into()?);
        if action != 0 {
            bail!("Response action is not Connect");
        }

        Ok(i64::from_be_bytes(buf[8..16].try_into()?))
    }

    fn parse_url(url: &str) -> SocketAddr {
        let url = Url::parse(url).expect("Failed to parse URL");
        let host: &str = url.host_str().unwrap();
        let port = url.port().unwrap();
        format!("{}:{}", host, port)
            .to_socket_addrs()
            .expect("Failed to resolve host")
            .next()
            .unwrap()
    }

    fn connect(
        rng: &mut ThreadRng,
        socket: &UdpSocket,
        addr: &SocketAddr,
    ) -> Result<i64, Box<dyn Error>> {
        // Send connection request
        let trans_id: i32 = rng.gen();
        socket.send_to(&Self::build_conn_request(trans_id), addr)?;

        // Receive connection response
        Self::recv_conn_response(&socket, trans_id)
    }
}

#[async_trait]
impl Trackable for UDP {
    async fn announce(&mut self) -> Result<(u32, Vec<Peer>)> {
        {
            let mut rng = rand::thread_rng();
            let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
            let addr = Self::parse_url(&self.url);

            // Connect
            self.conn_id = Some(Self::connect(&mut rng, &socket, &addr)?);
        }

        self.request().await
    }

    async fn request(&self) -> Result<(u32, Vec<Peer>)> {
        let mut rng = rand::thread_rng();
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let addr = Self::parse_url(&self.url);

        // Send announce request
        let key: i32 = rng.gen();
        let trans_id: i32 = rng.gen();
        socket.send_to(
            &self.build_announce_request(trans_id, self.conn_id.unwrap(), key),
            &addr,
        )?;
        socket.send_to(
            &self.build_announce_request(trans_id, self.conn_id.unwrap(), key),
            &addr,
        )?;

        // Receive announce response
        let (interval, _leechers, _seeders, peers) =
            Self::recv_announce_response(&socket, trans_id)?;
        let (interval, _leechers, _seeders, peers) =
            Self::recv_announce_response(&socket, trans_id)?;

        Ok((interval, peers))
    }

    fn get_info_hash(&self) -> [u8; 20] {
        self.info_hash
    }

    fn get_peer_id(&self) -> String {
        self.peer_id.clone()
    }

    fn set_uploaded(&mut self, uploaded: u64) {
        self.uploaded = uploaded;
    }

    fn set_downloaded(&mut self, downloaded: u64) {
        self.downloaded = downloaded;
    }

    fn set_event(&mut self, event: String) {
        self.event = event;
    }
}
