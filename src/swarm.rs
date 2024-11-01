use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use async_channel::{self, Receiver, Sender};
use tokio::{
    net::TcpListener,
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
};

const MAX_CONCURRENT_HANDSHAKES: usize = 10;

use crate::{
    client::PendingPeer,
    file::Piece,
    peer::{Peer, PeerConnection},
};

pub struct Swarm {
    peer_reciever: mpsc::Receiver<PendingPeer>,
    channel: (Sender<Piece>, Receiver<Piece>),
    peers: HashSet<Peer>,
    my_id: String,
}

impl Swarm {
    pub fn new(peer_reciever: mpsc::Receiver<PendingPeer>, my_id: String) -> Self {
        Self {
            peer_reciever,
            channel: async_channel::unbounded(),
            peers: HashSet::new(),
            my_id,
        }
    }

    pub async fn start(&mut self, info_hash: &'static [u8]) {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        loop {
            println!("Waiting for new peer...");
            let new_peer = self.peer_reciever.recv().await.unwrap();

            // Check if peer is already connected
            match &new_peer {
                PendingPeer::Incoming(peer, _) => {
                    if self.peers.contains(peer) {
                        continue;
                    }
                }
                PendingPeer::Outgoing(peer) => {
                    if self.peers.contains(peer) {
                        continue;
                    }
                }
            }
            println!("Adding new peer: {:?}", new_peer);

            // Limit the number of concurrent handshakes
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            self.introduce_peer(new_peer, info_hash, permit).await;
        }
    }

    pub async fn listen_for_peers(sender: mpsc::Sender<PendingPeer>, port: u16) -> Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("Incoming connection from: {:?}", addr);
            let peer = Peer::from_socket_address(addr);

            sender
                .send(PendingPeer::Incoming(peer, stream))
                .await
                .unwrap();
        }
    }

    async fn introduce_peer(
        &self,
        peer: PendingPeer,
        info_hash: &'static [u8],
        _permit: OwnedSemaphorePermit,
    ) {
        let id = self.my_id.clone();
        tokio::spawn(async move {
            let mut peer_connection = match PeerConnection::new(peer, id, info_hash).await {
                Ok(connection) => connection,
                Err(e) => {
                    println!("Failed to establish connection: {:?}", e);
                    return;
                }
            };

            println!(
                "Connection established with peer: {:?}",
                peer_connection.peer
            );

            peer_connection.start().await;
        });
    }
}
