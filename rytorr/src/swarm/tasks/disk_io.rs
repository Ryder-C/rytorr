use async_channel::Receiver;
use bit_vec::BitVec;
use std::sync::Arc;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::{mpsc::UnboundedSender, Mutex},
};
use tracing::{error, info, instrument, trace, Instrument};

use crate::file::Piece;

// Async task to write pieces to disk and update global have bitfield
#[instrument(skip(
    piece_receiver,
    have,
    piece_length,
    num_pieces,
    file_path,
    completed_tx
))]
pub(crate) async fn disk_writer_loop(
    file_path: String,
    piece_receiver: Receiver<Piece>,
    have: Arc<Mutex<BitVec>>,
    piece_length: u64,
    num_pieces: usize,
    completed_tx: UnboundedSender<usize>,
) {
    let mut file = match OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&file_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!(path = %file_path, error = %e, "Disk writer failed to open file, exiting task.");
            return;
        }
    };

    while let Ok(piece) = piece_receiver.recv().await {
        let span = tracing::info_span!("disk_write", piece_index = piece.index);

        span.in_scope(|| {
            trace!("Starting disk write operation");
        });

        let offset = piece.index as u64 * piece_length;

        if let Err(e) = file
            .seek(SeekFrom::Start(offset))
            .instrument(span.clone())
            .await
        {
            error!(parent: &span, error = %e, offset, "Disk seek failed");
            continue; // Skip this piece if seek fails
        }
        // Write the piece to disk
        if let Err(e) = file.write_all(&piece.data).instrument(span.clone()).await {
            error!(parent: &span, error = %e, "Disk write failed");
            continue; // Skip this piece if write fails
        }
        let have_count = {
            let mut h = have.lock().instrument(span.clone()).await;
            h.set(piece.index as usize, true);
            h.iter().filter(|b| *b).count()
        };
        span.in_scope(|| {
            info!(
                have_count = have_count,
                total_pieces = num_pieces,
                "Piece written to disk"
            );
        });
        if let Err(e) = completed_tx.send(piece.index as usize) {
            error!(error = %e, piece_index = piece.index, "Failed to send completed piece notification");
        }
    }
    info!("Disk writer loop finished");
}
