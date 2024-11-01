use bit_vec::BitVec;

const PSTR: &str = "BitTorrent protocol";

pub enum Message {
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

impl Message {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            Message::KeepAlive => vec![0, 0, 0, 0],
            Message::Choke => vec![0, 0, 0, 1, 0],
            Message::Unchoke => vec![0, 0, 0, 1, 1],
            Message::Interested => vec![0, 0, 0, 1, 2],
            Message::NotInterested => vec![0, 0, 0, 1, 3],
            Message::Have(piece_index) => {
                let mut buf = vec![0, 0, 0, 5, 4];
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf
            }
            Message::Bitfield(bitfield) => {
                let mut buf = vec![0, 0, 0, 1 + bitfield.len() as u8, 5];
                buf.extend(bitfield.to_bytes());
                buf
            }
            Message::Request(piece_index, begin, length) => {
                let mut buf = vec![0, 0, 0, 13, 6];
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            Message::Piece(piece_index, begin, block) => {
                let mut buf = vec![0, 0, 0, 9 + block.len() as u8, 7];
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend(block);
                buf
            }
            Message::Cancel(piece_index, begin, length) => {
                let mut buf = vec![0, 0, 0, 13, 8];
                buf.extend_from_slice(&piece_index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            Message::Port(port) => {
                let mut buf = vec![0, 0, 0, 3, 9];
                buf.extend_from_slice(&port.to_be_bytes());
                buf
            }
        }
    }
}
