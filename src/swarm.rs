use std::{error::Error, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::{mpsc, Mutex},
};

use crate::peer::Peer;

pub enum Message {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request(u32, u32, u32),
    Piece(u32, u32, Vec<u8>),
    Cancel(u32, u32, u32),
}

impl Message {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let id = bytes[0];
        match id {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have(u32::from_be_bytes([
                bytes[1], bytes[2], bytes[3], bytes[4],
            ]))),
            5 => Ok(Self::Bitfield(bytes[1..].to_vec())),
            6 => Ok(Self::Request(
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
                u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]),
                u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]),
            )),
            7 => Ok(Self::Piece(
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
                u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]),
                bytes[9..].to_vec(),
            )),
            8 => Ok(Self::Cancel(
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
                u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]),
                u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]),
            )),
            _ => Err("Invalid message id".into()),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        todo!()
    }
}

pub struct Swarm {
    pub peers: Vec<Arc<Mutex<Peer>>>,
    pub peers: Vec<Arc<Mutex<Peer>>>,
    pub uploaded: u64,
    pub downloaded: u64,

    lookup_peers: HashSet<Peer>,
    info_hash: Vec<u8>,
    peer_id: String,
}

impl Swarm {
    pub fn new(peers: Vec<Peer>) -> Self {
        let peers = peers
            .into_iter()
            .map(|peer| Arc::new(Mutex::new(peer)))
            .collect();
        Self {
            peers: peers
                .clone()
                .into_iter()
                .map(|peer| Arc::new(Mutex::new(peer)))
                .collect(),
            uploaded: 0,
            downloaded: 0,
            lookup_peers: peers.into_iter().collect(),
            info_hash,
            peer_id,
        }
    }

    pub async fn spawn_connections(&mut self, info_hash: &[u8], peer_id: &str) {
        for peer in &self.peers {
            let peer = Arc::clone(peer);

            // Try to connect to peer
            let stream = match peer.lock().await.connect(info_hash, peer_id).await {
                Ok(stream) => stream,
                Err(e) => {
                    println!("Error connecting to peer: {}", e);
                    continue;
                }
            };
            // Split stream into read and write halves
            let (read_stream, write_stream) = stream.into_split();

            // Create channel for communication of messages
            let (tx, rx) = mpsc::channel::<Message>(8);

            // Spawn reader task
            tokio::spawn(async move { Self::read_from_peer(read_stream).await });

            // Spawn writer task
            tokio::spawn(async move { Self::write_to_peer(write_stream, rx).await });
        }
    }

    async fn read_from_peer(mut read_stream: OwnedReadHalf) {
        let mut saved_buf = vec![];
        let mut buf = [0u8; 1024];

        loop {
            match read_stream.read(&mut buf).await {
                Ok(n) => {
                    if n == 0 {
                        continue;
                    }
                    saved_buf.extend_from_slice(&buf[..n]);
                }
                Err(e) => {
                    panic!("Error reading from peer: {}", e);
                }
            }

            let msg_len = u32::from_be_bytes(saved_buf[..4].try_into().unwrap());
            if saved_buf.len() < msg_len as usize {
                continue;
            }

            let msg = Message::from_bytes(&saved_buf[4..msg_len as usize + 4]).unwrap();
        }
    }

    async fn write_to_peer(mut write_stream: OwnedWriteHalf, mut rx: mpsc::Receiver<Message>) {
        loop {
            let msg = rx.recv().await.unwrap();
            let bytes = msg.to_bytes();
            write_stream.write_all(&bytes).await.unwrap();
        }
    }
}
