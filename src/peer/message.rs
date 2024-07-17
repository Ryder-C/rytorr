use bit_vec::BitVec;

const PSTR: &str = "BitTorrent protocol";

pub enum Message {
    Handshake(HandshakeMessage),
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(BitVec),
    Request(u32, u32, u32),
    Piece(u32, u32, Vec<u8>),
    Cancel(u32, u32, u32),
    Port(u16),
}

pub struct HandshakeMessage {
    info_hash: &'static [u8],
    peer_id: String,
}
