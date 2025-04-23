use crate::{
    file::Piece,
    peer::{PeerConnection, BLOCK_SIZE},
    swarm::{
        BitfieldEventHandler, ChokeEventHandler, HaveEventHandler, PeerEventHandler,
        UnchokeEventHandler,
    },
};
use async_trait::async_trait;
use bit_vec::BitVec;
use sha1::{Digest, Sha1};
use tracing::{debug, error, info, trace, warn};

// Define the trait that all message handlers will implement.
// It takes a mutable reference to the PeerConnection to modify its state.
#[async_trait]
pub(super) trait MessageHandler: Send {
    async fn handle(&self, connection: &mut PeerConnection);
}

// --- Implementations for each message type ---

pub(super) struct KeepAliveHandler;

#[async_trait]
impl MessageHandler for KeepAliveHandler {
    async fn handle(&self, _connection: &mut PeerConnection) {
        trace!("Received KeepAlive");
        // KeepAlive requires no state change
    }
}

pub(super) struct ChokeHandler;

#[async_trait]
impl MessageHandler for ChokeHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!("Received Choke");
        connection.peer_choking = true;
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(ChokeEventHandler {
            peer: connection.peer.clone(),
        });
        let _ = connection.event_tx.send(handler);
    }
}

pub(super) struct UnchokeHandler;

#[async_trait]
impl MessageHandler for UnchokeHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!("Received Unchoke");
        connection.peer_choking = false;
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(UnchokeEventHandler {
            peer: connection.peer.clone(),
        });
        let _ = connection.event_tx.send(handler);
    }
}

pub(super) struct InterestedHandler;

#[async_trait]
impl MessageHandler for InterestedHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!("Received Interested");
        connection.peer_interested = true;
        // TODO: Implement unchoking logic (send Unchoke if we want to serve this peer)
        // if !connection.am_choking { ... send Unchoke ... }
    }
}

pub(super) struct NotInterestedHandler;

#[async_trait]
impl MessageHandler for NotInterestedHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!("Received NotInterested");
        connection.peer_interested = false;
    }
}

pub(super) struct HaveHandler(pub u32); // Made field public for construction in message.rs

#[async_trait]
impl MessageHandler for HaveHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        let idx = self.0;
        debug!(piece_index = idx, "Received Have");
        if let Some(ref mut bf) = connection.peer_bitfield {
            if idx < bf.len() as u32 {
                bf.set(idx as usize, true);
            } else {
                warn!(
                    piece_index = idx,
                    bitfield_len = bf.len(),
                    "Received Have for index out of bounds"
                );
            }
        } else {
            warn!("Received Have before Bitfield");
        }
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(HaveEventHandler {
            peer: connection.peer.clone(),
            idx: idx as usize,
        });
        let _ = connection.event_tx.send(handler);
    }
}

pub(super) struct BitfieldHandler(pub BitVec); // Made field public

#[async_trait]
impl MessageHandler for BitfieldHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        let bf = self.0.clone(); // Clone to avoid borrowing issues
        debug!(bitfield_len = bf.len(), "Received Bitfield");
        if connection.peer_bitfield.is_some() {
            warn!("Received Bitfield message twice");
        }
        // TODO: Verify bitfield length against number of pieces from torrent info
        connection.peer_bitfield = Some(bf.clone());
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(BitfieldEventHandler {
            peer: connection.peer.clone(),
            bf,
        });
        let _ = connection.event_tx.send(handler);
    }
}

pub(super) struct RequestHandler {
    // Made fields public
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[async_trait]
impl MessageHandler for RequestHandler {
    async fn handle(&self, _connection: &mut PeerConnection) {
        debug!(
            piece_index = self.index,
            begin = self.begin,
            len = self.length,
            "Received Request"
        );
        // TODO: Implement logic to read from file and send Piece message
        warn!("Piece request handling not implemented");
    }
}

pub(super) struct PieceHandler {
    // Made fields public
    pub index: u32,
    pub begin: u32,
    pub block: Vec<u8>,
}

#[async_trait]
impl MessageHandler for PieceHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        let piece_index = self.index as usize; // Use usize for map keys
        let begin = self.begin;
        let block = self.block.clone(); // Clone data to avoid borrow issues if needed later
        let block_len = block.len() as u32;

        debug!(piece_index, begin, block_len, "Received Piece block");

        // --- Update Shared State ---
        let block_key = (piece_index, begin);
        let removed = {
            let mut pending_guard = connection.pending_requests.lock().await;
            pending_guard.remove(&block_key).is_some()
        };
        if removed {
            trace!(piece_index, begin, "Removed block from pending requests");
        } else {
            // This might happen if the request timed out just before the block arrived.
            // Or if we received an unsolicited block.
            warn!(
                piece_index,
                begin, "Received block that was not pending (or timed out)"
            );
        }

        let next_expected_begin = begin + block_len;
        {
            let mut progress_guard = connection.piece_download_progress.lock().await;
            // Update progress only if this block moves the progress forward
            let current_progress = progress_guard.entry(piece_index).or_insert(0);
            if next_expected_begin > *current_progress {
                *current_progress = next_expected_begin;
                trace!(
                    piece_index,
                    new_progress = next_expected_begin,
                    "Updated download progress"
                );
            }
        }

        let num_blocks = connection.piece_length.div_ceil(BLOCK_SIZE);
        let blocks = connection
            .pending_pieces
            .entry(piece_index)
            .or_insert_with(|| vec![None; num_blocks]);
        let block_index = (begin as usize) / BLOCK_SIZE;
        if block_index >= blocks.len() {
            error!(
                piece_index,
                begin, block_len, num_blocks, "Received block with invalid begin offset"
            );
            return;
        }
        if blocks[block_index].is_some() {
            trace!(
                piece_index,
                block_index,
                "Overwriting existing block data (was duplicate or race?)"
            );
        }
        // Store the block
        blocks[block_index] = Some(block);

        // Check if the piece is complete
        if blocks.iter().all(Option::is_some) {
            trace!(piece_index, "All blocks received, assembling piece");
            let mut data = Vec::with_capacity(connection.piece_length);
            // We use take() here to consume the blocks as we assemble the piece,
            // freeing memory and preventing accidental reuse.
            for b_opt in blocks.iter_mut() {
                if let Some(b_data) = b_opt.take() {
                    data.extend(b_data);
                } else {
                    // This should theoretically not happen due to the all() check,
                    // but handle defensively.
                    error!(
                        piece_index,
                        block_index = data.len() / BLOCK_SIZE,
                        "Missing block during assembly after all() check!"
                    );
                    // Clear the entry to force re-request later?
                    connection.pending_pieces.remove(&{ piece_index });
                    return;
                }
            }

            // Remove the entry now that we've taken the blocks
            connection.pending_pieces.remove(&{ piece_index });

            // TODO: Adjust data length if it's the last piece and shorter than piece_length
            // Example: let expected_piece_len = if piece_index as usize == connection.piece_hashes.len() - 1 { ... } else { connection.piece_length };
            // data.truncate(expected_piece_len);

            trace!(piece_index, "Verifying piece hash");
            let mut hasher = Sha1::new();
            hasher.update(&data);
            let calculated_hash = hasher.finalize();
            let expected_hash = &connection.piece_hashes[piece_index];

            if calculated_hash.as_slice() == expected_hash {
                info!(
                    piece_index,
                    data_len = data.len(),
                    "Piece completed and verified"
                );
                if let Err(e) = connection
                    .piece_sender
                    .send(Piece {
                        index: piece_index as u32,
                        data,
                    })
                    .await
                {
                    error!(error = %e, piece_index, "Failed to send completed piece to channel");
                }
                // TODO: Send Have message? Update local bitfield?
                // Example: connection.send_message(message::Message::Have(piece_index)).await;
                // if let Some(have_bf) = connection.have_bitfield.as_mut() { have_bf.set(piece_index as usize, true); }
            } else {
                error!(piece_index, expected_hash = ?expected_hash, calculated_hash = ?calculated_hash.as_slice(), "Piece failed hash check!");
                // Piece data is already removed from pending_pieces.
                // The scheduler will eventually re-request it if needed.
                // TODO: Consider penalizing the peer?
            }
        }
    }
}

pub(super) struct CancelHandler {
    // Made fields public
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[async_trait]
impl MessageHandler for CancelHandler {
    async fn handle(&self, _connection: &mut PeerConnection) {
        debug!(
            piece_index = self.index,
            begin = self.begin,
            len = self.length,
            "Received Cancel"
        );
        // TODO: Implement cancellation logic if supporting pipelined requests
        warn!("Cancel handling not implemented");
    }
}

pub(super) struct PortHandler(pub u16); // Made field public

#[async_trait]
impl MessageHandler for PortHandler {
    async fn handle(&self, _connection: &mut PeerConnection) {
        debug!(dht_port = self.0, "Received Port message (DHT)");
        // TODO: Implement DHT integration if needed
        warn!("Port message handling not implemented");
    }
}
