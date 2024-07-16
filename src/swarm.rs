use async_channel::{self, Receiver, Sender};

use crate::{file::Piece, peer::Peer};

pub struct Swarm {
    channel: (Sender<Piece>, Receiver<Piece>),
}

impl Swarm {
    pub fn new() -> Self {
        Self {
            channel: async_channel::unbounded(),
        }
    }

    pub fn introduce_peer(&self, peer: Peer) {
        let reciever = self.channel.1.clone();
        tokio::spawn(async move {});
    }
}
