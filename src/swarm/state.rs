use bit_vec::BitVec;

/// Per-peer state tracked by the Swarm
#[derive(Clone, Debug)]
pub(crate) struct PeerState {
    pub(crate) bitfield: BitVec,
    pub(crate) is_unchoked: bool,
}

impl PeerState {
    // Optional: Add a constructor if useful, e.g., for default state
    pub(crate) fn new(num_pieces: usize) -> Self {
        Self {
            bitfield: BitVec::from_elem(num_pieces, false),
            is_unchoked: false,
        }
    }
}
