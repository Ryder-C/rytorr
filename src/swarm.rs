use std::collections::HashSet;

use anyhow::Result;
use async_channel::{self, Receiver, Sender};
use tokio::{net::TcpListener, sync::mpsc};

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
        loop {
            let new_peer = self.peer_reciever.recv().await.unwrap();
            println!("Adding new peer: {:?}", new_peer);
            self.introduce_peer(new_peer, info_hash);
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

    fn introduce_peer(&self, peer: PendingPeer, info_hash: &'static [u8]) {
        let receiver = self.channel.1.clone();
        let id = self.my_id.clone();
        tokio::spawn(async move {
            let mut peer_connection = PeerConnection::new(peer, id, info_hash).await.unwrap();
        });
    }
}
