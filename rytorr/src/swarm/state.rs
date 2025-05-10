use bit_vec::BitVec;
use std::time::Instant;

/// Per-peer state tracked by the Swarm
#[derive(Clone, Debug)]
pub(crate) struct PeerState {
    pub(crate) bitfield: BitVec,
    pub(crate) am_choking_peer: bool,
    pub(crate) peer_is_choking_us: bool,
    pub(crate) peer_is_interested_in_us: bool,
    pub(crate) we_are_interested_in_peer: bool,

    pub(crate) total_uploaded_to_peer: u64,
    pub(crate) total_downloaded_from_peer: u64,
    pub(crate) last_choke_cycle_uploaded: u64,
    pub(crate) last_choke_cycle_downloaded: u64,

    pub(crate) last_message_sent_at: Instant,
}

impl PeerState {
    pub(crate) fn new(num_pieces: usize) -> Self {
        Self {
            bitfield: BitVec::from_elem(num_pieces, false),
            am_choking_peer: true,
            peer_is_choking_us: true,
            peer_is_interested_in_us: false,
            we_are_interested_in_peer: false,
            total_uploaded_to_peer: 0,
            total_downloaded_from_peer: 0,
            last_choke_cycle_uploaded: 0,
            last_choke_cycle_downloaded: 0,
            last_message_sent_at: Instant::now(),
        }
    }

    pub(crate) fn upload_rate_to_peer_since_last_cycle(&self) -> u64 {
        self.total_uploaded_to_peer
            .saturating_sub(self.last_choke_cycle_uploaded)
    }

    pub(crate) fn download_rate_from_peer_since_last_cycle(&self) -> u64 {
        self.total_downloaded_from_peer
            .saturating_sub(self.last_choke_cycle_downloaded)
    }

    pub(crate) fn end_choke_cycle_snapshot(&mut self) {
        self.last_choke_cycle_uploaded = self.total_uploaded_to_peer;
        self.last_choke_cycle_downloaded = self.total_downloaded_from_peer;
    }
}
