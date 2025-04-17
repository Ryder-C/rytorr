use bit_vec::BitVec;
use anyhow::{Result, bail};

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
    /// Helper: write length prefix, id, and payload according to the spec
    fn encode_msg(id: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 1 + payload.len());
        let len = 1 + payload.len() as u32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.push(id);
        buf.extend_from_slice(payload);
        buf
    }

    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            Message::KeepAlive => vec![0, 0, 0, 0],
            Message::Choke => Self::encode_msg(0, &[]),
            Message::Unchoke => Self::encode_msg(1, &[]),
            Message::Interested => Self::encode_msg(2, &[]),
            Message::NotInterested => Self::encode_msg(3, &[]),
            Message::Have(piece_index) => {
                Self::encode_msg(4, &piece_index.to_be_bytes())
            }
            Message::Bitfield(bitfield) => {
                let bit_bytes = bitfield.to_bytes();
                Self::encode_msg(5, &bit_bytes)
            }
            Message::Request(piece_index, begin, length) => {
                let mut payload = Vec::with_capacity(12);
                payload.extend_from_slice(&piece_index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                Self::encode_msg(6, &payload)
            }
            Message::Piece(piece_index, begin, block) => {
                let mut payload = Vec::with_capacity(8 + block.len());
                payload.extend_from_slice(&piece_index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend(block);
                Self::encode_msg(7, &payload)
            }
            Message::Cancel(piece_index, begin, length) => {
                let mut payload = Vec::with_capacity(12);
                payload.extend_from_slice(&piece_index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                Self::encode_msg(8, &payload)
            }
            Message::Port(port) => {
                Self::encode_msg(9, &port.to_be_bytes())
            }
        }
    }

    /// Parse a raw message payload (excluding the 4-byte length) into a Message enum.
    pub fn from_be_bytes(buf: &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Ok(Message::KeepAlive);
        }
        let id = buf[0];
        match id {
            0 => Ok(Message::Choke),
            1 => Ok(Message::Unchoke),
            2 => Ok(Message::Interested),
            3 => Ok(Message::NotInterested),
            4 => {
                let idx = u32::from_be_bytes(buf[1..5].try_into().unwrap());
                Ok(Message::Have(idx))
            }
            5 => {
                let bytes = buf[1..].to_vec();
                let bf = BitVec::from_bytes(&bytes);
                Ok(Message::Bitfield(bf))
            }
            6 => {
                let pi = u32::from_be_bytes(buf[1..5].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[5..9].try_into().unwrap());
                let len = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                Ok(Message::Request(pi, begin, len))
            }
            7 => {
                // Piece: index (4), begin (4), block (...)
                let pi = u32::from_be_bytes(buf[1..5].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[5..9].try_into().unwrap());
                let block = buf[9..].to_vec();
                Ok(Message::Piece(pi, begin, block))
            }
            8 => {
                let pi = u32::from_be_bytes(buf[1..5].try_into().unwrap());
                let begin = u32::from_be_bytes(buf[5..9].try_into().unwrap());
                let len = u32::from_be_bytes(buf[9..13].try_into().unwrap());
                Ok(Message::Cancel(pi, begin, len))
            }
            9 => {
                let port = u16::from_be_bytes(buf[1..3].try_into().unwrap());
                Ok(Message::Port(port))
            }
            other => bail!("Unsupported message id {}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bit_vec::BitVec;

    #[test]
    fn test_keepalive() {
        let ba = Message::KeepAlive.to_be_bytes();
        assert_eq!(ba, vec![0,0,0,0]);
        let msg = Message::from_be_bytes(&[]).unwrap();
        assert!(matches!(msg, Message::KeepAlive));
    }

    #[test]
    fn test_choke_unchoke_interested_notinterested() {
        assert_eq!(Message::Choke.to_be_bytes()[4], 0);
        assert_eq!(Message::Unchoke.to_be_bytes()[4], 1);
        assert_eq!(Message::Interested.to_be_bytes()[4], 2);
        assert_eq!(Message::NotInterested.to_be_bytes()[4], 3);
    }

    #[test]
    fn test_have() {
        let idx = 123;
        let ba = Message::Have(idx).to_be_bytes();
        // length(5), id(4), piece index
        assert_eq!(&ba[0..5], &[0,0,0,5,4]);
        let parsed = Message::from_be_bytes(&ba[4..]).unwrap();
        if let Message::Have(i) = parsed { assert_eq!(i, idx); } else { panic!() }
    }

    #[test]
    fn test_bitfield() {
        let data = [0b1010_0001, 0b0000_0111];
        let bv = BitVec::from_bytes(&data);
        let ba = Message::Bitfield(bv.clone()).to_be_bytes();
        // len = 1 + bytes
        let len = ((ba[0] as u32) << 24) | ((ba[1] as u32) << 16) | ((ba[2] as u32) << 8) | (ba[3] as u32);
        assert_eq!(len as usize, 1 + data.len());
        assert_eq!(ba[4], 5);
        let parsed = Message::from_be_bytes(&ba[4..]).unwrap();
        if let Message::Bitfield(pb) = parsed { assert_eq!(pb.to_bytes(), data); } else { panic!() }
    }

    #[test]
    fn test_request() {
        let m = Message::Request(1,2,3);
        let ba = m.to_be_bytes();
        let len = ((ba[0] as u32) << 24) | ((ba[1] as u32) << 16) | ((ba[2] as u32) << 8) | (ba[3] as u32);
        assert_eq!(len, 13);
        assert_eq!(ba[4], 6);
        let parsed = Message::from_be_bytes(&ba[4..]).unwrap();
        if let Message::Request(a,b,c) = parsed { assert_eq!((a,b,c),(1,2,3)); } else { panic!() }
    }

    #[test]
    fn test_piece_cancel_port() {
        // piece
        let block = vec![9,8,7];
        let m = Message::Piece(10,20,block.clone());
        let ba = m.to_be_bytes();
        assert_eq!(ba[4],7);
        // cancel
        let m2 = Message::Cancel(5,6,7);
        let ba2 = m2.to_be_bytes();
        assert_eq!(ba2[4],8);
        // port
        let m3 = Message::Port(6881);
        let ba3 = m3.to_be_bytes();
        assert_eq!(ba3[4],9);
    }
}
