use crate::peer::{
    message::{self},
    PeerConnection,
};
use async_trait::async_trait;
use tracing::{debug, error};

// --- Swarm Command Handling ---

/// Trait for commands sent from the Swarm to a PeerConnection task.
#[async_trait]
pub(crate) trait SwarmCommandHandler: Send + std::fmt::Debug {
    async fn handle(&self, connection: &mut PeerConnection);
}

/// Command sent from Scheduler to PeerConnection to request a block.
#[derive(Debug)]
pub(crate) struct RequestCommandHandler {
    pub(crate) piece: u32,
    pub(crate) begin: u32,
    pub(crate) length: u32,
}

#[async_trait]
impl SwarmCommandHandler for RequestCommandHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(
            piece_index = self.piece,
            begin = self.begin,
            length = self.length,
            "Handling Request command"
        );
        if let Err(e) = connection
            .send_message(message::Message::Request(
                self.piece,
                self.begin,
                self.length,
            ))
            .await
        {
            error!(error = %e, peer.ip = %connection.peer.ip, "Failed to send Request message");
            // Consider breaking the loop or notifying swarm of failure
        }
    }
}

/// Command sent from Swarm broadcast loop to a PeerConnection task to send a Have message.
#[derive(Debug)]
pub(crate) struct SendHaveCommand {
    pub(crate) piece_index: u32,
}

#[async_trait]
impl SwarmCommandHandler for SendHaveCommand {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(piece_index = self.piece_index, peer.ip = %connection.peer.ip, "Handling SendHave command");
        if let Err(e) = connection
            .send_message(message::Message::Have(self.piece_index))
            .await
        {
            error!(error = %e, piece_index = self.piece_index, peer.ip = %connection.peer.ip, "Failed to send Have message");
        }
    }
}

/// Command sent from Swarm to a PeerConnection task to choke the peer.
#[derive(Debug)]
pub(crate) struct ChokePeerCommand;

#[async_trait]
impl SwarmCommandHandler for ChokePeerCommand {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(peer.ip = %connection.peer.ip, "Handling ChokePeer command");
        if let Err(e) = connection.set_choking().await {
            error!(error = %e, peer.ip = %connection.peer.ip, "Failed to execute set_choking command");
            // TODO: Handle error, maybe notify Swarm or disconnect?
        }
    }
}

/// Command sent from Swarm to a PeerConnection task to unchoke the peer.
#[derive(Debug)]
pub(crate) struct UnchokePeerCommand;

#[async_trait]
impl SwarmCommandHandler for UnchokePeerCommand {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(peer.ip = %connection.peer.ip, "Handling UnchokePeer command");
        if let Err(e) = connection.set_unchoking().await {
            error!(error = %e, peer.ip = %connection.peer.ip, "Failed to execute set_unchoking command");
            // TODO: Handle error
        }
    }
}
