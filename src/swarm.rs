use std::{collections::HashSet, sync::Arc};

use async_channel::{self, Receiver, Sender};
use tokio::sync::{mpsc, Mutex};

use crate::{file::Piece, peer::Peer};

pub struct Swarm {
    peer_reciever: mpsc::Receiver<Vec<Peer>>,
    channel: (Sender<Piece>, Receiver<Piece>),
    peers: HashSet<Peer>,
}

impl Swarm {
    pub fn new(peer_reciever: mpsc::Receiver<Vec<Peer>>) -> Self {
        Self {
            peer_reciever,
            channel: async_channel::unbounded(),
            peers: HashSet::new(),
        }
    }

    pub async fn start(&mut self) {
        loop {
            let new_peers = self.peer_reciever.recv().await.unwrap();
            for peer in new_peers {
                if self.peers.insert(peer.clone()) {
                    // Cloning peer here is not ideal
                    self.introduce_peer(peer);
                }
            }
        }
    }

    fn introduce_peer(&self, peer: Peer) {
        let reciever = self.channel.1.clone();
        tokio::spawn(async move {});
    }
}
