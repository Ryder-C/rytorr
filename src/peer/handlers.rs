use crate::{
    file::Piece,
    peer::{message, PeerConnection, BLOCK_SIZE},
    swarm::tasks::event_processor::{
        BitfieldEventHandler, ChokeEventHandler, HaveEventHandler, PeerEventHandler,
        UnchokeEventHandler,
    },
};
use async_trait::async_trait;
use bit_vec::BitVec;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
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
        connection.connection_state.peer_choking = true;
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
        connection.connection_state.peer_choking = false;
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
        connection.connection_state.peer_interested = true;
        // TODO: Implement unchoking logic (send Unchoke if we want to serve this peer)
        // if !connection.connection_state.am_choking { ... send Unchoke ... }
    }
}

pub(super) struct NotInterestedHandler;

#[async_trait]
impl MessageHandler for NotInterestedHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!("Received NotInterested");
        connection.connection_state.peer_interested = false;
    }
}

pub(super) struct HaveHandler(pub u32); // Made field public for construction in message.rs

#[async_trait]
impl MessageHandler for HaveHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        let idx = self.0;
        debug!(piece_index = idx, "Received Have");
        let mut interest_needs_update = false; // Flag to check if interest state might change
        if let Some(ref mut bf) = connection.peer_bitfield {
            if idx < bf.len() as u32 {
                // Only update interest if the bit wasn't already set
                if !bf.get(idx as usize).unwrap_or(false) {
                    bf.set(idx as usize, true);
                    interest_needs_update = true;
                }
            } else {
                warn!(
                    piece_index = idx,
                    bitfield_len = bf.len(),
                    "Received Have for index out of bounds"
                );
            }
        } else {
            debug!("Received Have before Bitfield");
            // Some clients share their have states slowly, so we can ignore this.
        }

        // Send Have event regardless
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(HaveEventHandler {
            peer: connection.peer.clone(),
            idx: idx as usize,
        });
        let _ = connection.event_tx.send(handler);

        // Update interest state if the peer's bitfield changed
        if interest_needs_update {
            if let Err(e) = connection.update_interest_state().await {
                error!(error = %e, peer.ip = %connection.peer.ip, "Failed to update interest state after Have");
                // Handle error appropriately, maybe mark connection as failed?
            }
        }
    }
}

pub(super) struct BitfieldHandler(pub BitVec);

#[async_trait]
impl MessageHandler for BitfieldHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        let bf = self.0.clone(); // Clone to avoid borrowing issues
        debug!(bitfield_len = bf.len(), "Received Bitfield");

        if connection.peer_bitfield.is_some() {
            // Check if this is the first bitfield
            warn!("Received Bitfield message twice");
        }
        // TODO: Verify bitfield length against number of pieces from torrent info
        connection.peer_bitfield = Some(bf.clone());

        // Send Bitfield event
        let handler: Box<dyn PeerEventHandler + Send> = Box::new(BitfieldEventHandler {
            peer: connection.peer.clone(),
            bf,
        });
        let _ = connection.event_tx.send(handler);

        // Always update interest state after receiving a bitfield
        if let Err(e) = connection.update_interest_state().await {
            error!(error = %e, peer.ip = %connection.peer.ip, "Failed to update interest state after Bitfield");
            // Handle error
        }
    }
}

pub(super) struct RequestHandler {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

#[async_trait]
impl MessageHandler for RequestHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        debug!(
            peer.ip = %connection.peer.ip,
            piece_index = self.index,
            begin = self.begin,
            len = self.length,
            "Received Request"
        );

        // Validation
        if connection.connection_state.am_choking {
            trace!(peer.ip = %connection.peer.ip, piece_index = self.index, "Ignoring request: We are choking peer (am_choking=true)");
            return;
        } else {
            trace!(peer.ip = %connection.peer.ip, piece_index = self.index, "Processing request: We are not choking peer (am_choking=false)");
        }

        if self.length as usize > BLOCK_SIZE {
            warn!(peer.ip = %connection.peer.ip, requested_len = self.length, max_len = BLOCK_SIZE, "Ignoring request: Length too large");
            return;
        }
        // TODO: Add validation for index and begin against piece_length and num_pieces

        let block_len = self.length as usize;
        let mut block_data = vec![0u8; block_len];
        let offset = self.index as u64 * connection.piece_length as u64 + self.begin as u64;
        trace!(peer.ip = %connection.peer.ip, piece_index = self.index, offset, "Calculated file offset for request");

        // Lock file, seek, and read
        let mut file_guard = connection.read_file_handle.lock().await;
        trace!(peer.ip = %connection.peer.ip, piece_index = self.index, offset, "Acquired file lock for reading");
        if let Err(e) = file_guard.seek(SeekFrom::Start(offset)).await {
            error!(error = %e, peer.ip = %connection.peer.ip, offset, "Failed to seek in file for request");
            return; // Cannot fulfill request
        }
        trace!(peer.ip = %connection.peer.ip, piece_index = self.index, offset, "Seek successful");

        match file_guard.read_exact(&mut block_data).await {
            Ok(_) => {
                trace!(peer.ip = %connection.peer.ip, offset, len = block_len, "Read block from file successfully");
                drop(file_guard); // Drop lock before sending

                let piece_msg = message::Message::Piece(self.index, self.begin, block_data);
                let log_len = if let message::Message::Piece(_, _, ref data) = piece_msg {
                    data.len()
                } else {
                    0
                };
                trace!(peer.ip = %connection.peer.ip, piece_index = self.index, begin = self.begin, len = log_len, "Constructed Piece message, attempting send");
                if let Err(e) = connection.send_message(piece_msg).await {
                    error!(error = %e, peer.ip = %connection.peer.ip, "Failed to send Piece message");
                    return; // Connection might be dead
                }
                info!(peer.ip = %connection.peer.ip, piece_index = self.index, begin = self.begin, "Successfully sent Piece message");

                let mut uploaded = connection.uploaded.write().await; // Use renamed field
                *uploaded += block_len as u64;
                trace!(peer.ip = %connection.peer.ip, added = block_len, total_uploaded = *uploaded, "Updated uploaded count");
            }
            Err(e) => {
                error!(error = %e, peer.ip = %connection.peer.ip, offset, len = block_len, "Failed to read block from file for request");
            }
        }
    }
}

pub(super) struct PieceHandler {
    pub index: u32,
    pub begin: u32,
    pub block: Vec<u8>,
}

#[async_trait]
impl MessageHandler for PieceHandler {
    async fn handle(&self, connection: &mut PeerConnection) {
        trace!(peer.ip = %connection.peer.ip, piece_index = self.index, begin = self.begin, block_len = self.block.len(), "PieceHandler: Received piece message bytes.");
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
                let piece_len = data.len(); // Get the actual length of the verified piece
                debug!(
                    piece_index,
                    data_len = piece_len,
                    "Piece completed and verified"
                );

                // Increment the global downloaded counter using renamed field
                {
                    let mut downloaded = connection.downloaded.write().await;
                    *downloaded += piece_len as u64;
                    trace!(
                        piece_index,
                        added = piece_len,
                        total_downloaded = *downloaded,
                        "Updated global downloaded count"
                    );
                }

                // Update local 'have' bitfield BEFORE sending piece/updating interest
                let have_bitfield_changed = connection.set_piece_as_completed(piece_index);

                // Update interest state IF our 'have' status changed for this piece
                // (We might now be not interested if this was the last piece we needed from this peer)
                if have_bitfield_changed {
                    if let Err(e) = connection.update_interest_state().await {
                        error!(error = %e, peer.ip = %connection.peer.ip, "Failed to update interest state after piece completion");
                        // Potentially recoverable error, continue to send piece
                    }
                }

                // Send piece to disk writer channel
                if let Err(e) = connection
                    .piece_sender
                    .send(Piece {
                        index: piece_index as u32,
                        data,
                    })
                    .await
                {
                    error!(error = %e, piece_index, "Failed to send completed piece to channel");
                    // If we failed to send, should we revert the have_bitfield update? Probably not needed.
                    // Should we retry sending? Depends on channel setup.
                } else {
                    trace!(piece_index, "Sent completed piece to disk writer");
                }
                // Note: Have message is no longer sent from here.
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
