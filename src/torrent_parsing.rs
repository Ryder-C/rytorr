use bendy::decoding::{Error, FromBencode, Object};
use sha1::{Digest, Sha1};
use std::path::PathBuf;

use crate::{peer::Peer, tracking::http_tracker::Response};

#[derive(Debug)]
pub struct File {
    pub length: i64,
    pub path: PathBuf,
}

#[derive(Debug)]
pub struct Torrent {
    pub announce: Option<String>,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info_hash: [u8; 20],
    pub length: u64,
    pub files: Option<Vec<File>>,
    pub name: String,
    pub piece_length: i64,
    pub pieces: Vec<Vec<u8>>,
}

impl FromBencode for Torrent {
    fn decode_bencode_object(object: Object) -> Result<Self, Error> {
        let mut announce = None;
        let mut announce_list = None;
        let mut info_hash = [0; 20];
        let mut length = 0;
        let mut files = None;
        let mut name = String::new();
        let mut piece_length = 0;
        let mut pieces = Vec::new();

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"announce", value) => {
                    announce = Some(String::decode_bencode_object(value)?);
                }
                (b"announce-list", value) => {
                    let mut list = value.try_into_list()?;
                    while let Some(value) = list.next_object()? {
                        let mut tier = Vec::new();
                        let mut tier_list = value.try_into_list()?;
                        while let Some(value) = tier_list.next_object()? {
                            tier.push(String::decode_bencode_object(value)?);
                        }
                        announce_list.get_or_insert(Vec::new()).push(tier);
                    }
                }
                (b"info", value) => {
                    let mut info = value.try_into_dictionary()?;

                    while let Some(pair) = info.next_pair()? {
                        match pair {
                            (b"length", value) => {
                                length = u64::decode_bencode_object(value)?;
                            }
                            (b"files", value) => {
                                files = Some(Vec::decode_bencode_object(value)?);
                            }
                            (b"name", value) => {
                                name = String::decode_bencode_object(value)?;
                            }
                            (b"piece length", value) => {
                                piece_length = i64::decode_bencode_object(value)?;
                            }
                            (b"pieces", value) => {
                                // Pieces is a string of 20-byte SHA1 hash values
                                let bytes = value.try_into_bytes()?;
                                // Group bytes into 20 byte chunks and push them to pieces
                                for chunk in bytes.chunks(20) {
                                    pieces.push(chunk.to_vec());
                                }
                            }
                            _ => {}
                        }
                    }

                    let mut hasher = Sha1::new();
                    hasher.update(info.into_raw()?);
                    info_hash = hasher.finalize().into();
                }
                _ => {}
            }
        }

        Ok(Torrent {
            announce,
            announce_list,
            info_hash,
            length,
            files,
            name,
            piece_length,
            pieces,
        })
    }
}

impl FromBencode for File {
    fn decode_bencode_object(object: Object) -> Result<Self, Error> {
        let mut length = 0;
        let mut path = PathBuf::new();

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"length", value) => {
                    length = i64::decode_bencode_object(value)?;
                }
                (b"path", value) => {
                    path = Vec::decode_bencode_object(value)?
                        .into_iter()
                        .map(|bytes| String::from_utf8(bytes).unwrap())
                        .collect();
                }
                _ => {}
            }
        }

        Ok(File { length, path })
    }
}

impl FromBencode for Response {
    fn decode_bencode_object(object: Object) -> Result<Self, Error> {
        let mut failure_reason = None;
        let mut interval = None;
        let mut tracker_id = None;
        let mut seeders = None;
        let mut leechers = None;
        let mut peers = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"failure reason", value) => {
                    failure_reason = Some(String::decode_bencode_object(value)?);
                }
                (b"interval", value) => {
                    interval = Some(u32::decode_bencode_object(value)?);SS
                }
                (b"tracker id", value) => {
                    tracker_id = Some(String::decode_bencode_object(value)?);
                }
                (b"complete", value) => {
                    seeders = Some(u32::decode_bencode_object(value)?);
                }
                (b"incomplete", value) => {
                    leechers = Some(u32::decode_bencode_object(value)?);
                }
                (b"peers", value) => match value {
                    Object::Bytes(bytes) => {
                        for chunk in bytes.chunks(6) {
                            peers
                                .get_or_insert(Vec::new())
                                .push(Peer::from_be_bytes(&chunk));
                        }
                    }
                    Object::List(mut list) => {
                        while let Some(value) = list.next_object()? {
                            peers
                                .get_or_insert(Vec::new())
                                .push(Peer::decode_bencode_object(value)?);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        Ok(Response {
            failure_reason,
            interval,
            tracker_id,
            seeders,
            leechers,
            peers,
        })
    }
}

impl FromBencode for Peer {
    fn decode_bencode_object(object: Object) -> Result<Self, Error> {
        let mut dict = object.try_into_dictionary()?;
        let mut ip = String::new();
        let mut port = 0;
        let mut peer_id = None;

        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"ip", value) => {
                    ip = String::decode_bencode_object(value)?;
                }
                (b"port", value) => {
                    port = u16::decode_bencode_object(value)?;
                }
                (b"peer id", value) => {
                    peer_id = Some(value.try_into_bytes()?.try_into()?);
                }
                _ => {}
            }
        }

        Ok(Peer::new(ip, port, peer_id))
    }
}
