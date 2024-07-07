use anyhow::{ensure, Result};
use bendy::decoding::{FromBencode, Object, ResultExt};

#[derive(Debug)]
pub struct Peer {
    peer_id: Option<String>,
    ip: String,
    port: u16,
}

impl Peer {
    pub fn new(peer_id: String, ip: String, port: u16) -> Self {
        Self {
            peer_id: Some(peer_id),
            ip,
            port,
        }
    }

    pub fn from_be_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == 6, "can only decode peer from 6 bytes");

        let ip = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);
        Ok(Self {
            peer_id: None,
            ip,
            port,
        })
    }
}

impl FromBencode for Peer {
    fn decode_bencode_object(
        object: bendy::decoding::Object,
    ) -> Result<Self, bendy::decoding::Error>
    where
        Self: Sized,
    {
        let mut peer_id = None;
        let mut ip = None;
        let mut port = None;

        match object {
            Object::Dict(mut dict) => {
                while let Some(pair) = dict.next_pair()? {
                    match pair {
                        (b"peer id", value) => {
                            peer_id = String::decode_bencode_object(value)
                                .context("peer id")
                                .map(Some)?
                        }
                        (b"ip", value) => {
                            ip = String::decode_bencode_object(value)
                                .context("ip")
                                .map(Some)?
                        }
                        (b"port", value) => {
                            port = u16::decode_bencode_object(value)
                                .context("port")
                                .map(Some)?
                        }
                        _ => {}
                    }
                }
                let ip = ip.ok_or_else(|| bendy::decoding::Error::missing_field("ip"))?;
                let port = port.ok_or_else(|| bendy::decoding::Error::missing_field("port"))?;

                Ok(Self { peer_id, ip, port })
            }
            Object::Bytes(bytes) => Ok(Peer::from_be_bytes(bytes).unwrap()),
            _ => Err(bendy::decoding::Error::missing_field(
                "Object::Dict or Object::Bytes",
            )),
        }
    }
}
