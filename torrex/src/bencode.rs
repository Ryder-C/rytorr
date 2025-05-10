use std::{collections::HashSet, fs, path::PathBuf};

use bendy::decoding::{FromBencode, Object};
use chrono::{DateTime, Local, TimeZone};
use sha1::{Digest, Sha1};

type BendyResult<T> = Result<T, bendy::decoding::Error>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Torrent {
    pub info: Info,
    pub announce_list: Vec<String>,
    pub creation_date: Option<DateTime<Local>>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub encoding: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Info {
    pub name: String,
    pub files: Vec<File>,
    pub hash: [u8; 20],
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct File {
    pub length: u64,
    pub md5sum: Option<String>,
    pub path: PathBuf,
}

impl Torrent {
    pub fn new(path: &str) -> BendyResult<Self> {
        let file = fs::read(path)?;
        Torrent::from_bencode(&file)
    }
}

impl FromBencode for Torrent {
    fn decode_bencode_object(object: Object) -> BendyResult<Self>
    where
        Self: Sized,
    {
        let mut info = None;
        let mut announce_list = HashSet::new();
        let mut creation_date = None;
        let mut comment = None;
        let mut created_by = None;
        let mut encoding = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"info", value) => info = Some(Info::decode_bencode_object(value)?),
                (b"announce", value) => {
                    announce_list.insert(String::decode_bencode_object(value)?);
                }
                (b"announce-list", value) => {
                    let mut list_raw = value.try_into_list()?;
                    while let Some(value) = list_raw.next_object()? {
                        let mut tier_list = value.try_into_list()?;
                        while let Some(value) = tier_list.next_object()? {
                            announce_list.insert(String::decode_bencode_object(value)?);
                        }
                    }
                }
                (b"creation date", value) => {
                    creation_date = Some(
                        Local
                            .timestamp_opt(i64::decode_bencode_object(value)?, 0)
                            .unwrap(),
                    )
                }
                (b"comment", value) => comment = Some(String::decode_bencode_object(value)?),
                (b"created by", value) => created_by = Some(String::decode_bencode_object(value)?),
                (b"encoding", value) => encoding = Some(String::decode_bencode_object(value)?),
                _ => {}
            }
        }

        let info = info.expect("Decoding Error: Missing info dictionary");
        let announce_list = announce_list.into_iter().collect();

        Ok(Self {
            info,
            announce_list,
            creation_date,
            comment,
            created_by,
            encoding,
        })
    }
}

impl FromBencode for Info {
    fn decode_bencode_object(object: Object) -> BendyResult<Self>
    where
        Self: Sized,
    {
        let mut name = None;
        let mut files = None;

        let mut length = None;
        let mut md5sum = None;

        let mut piece_length = None;
        let mut pieces_raw = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"piece length", value) => piece_length = Some(u32::decode_bencode_object(value)?),
                (b"pieces", value) => pieces_raw = Some(value.try_into_bytes()?.to_vec()),
                (b"name", value) => name = Some(String::decode_bencode_object(value)?),
                (b"files", value) => files = Some(Vec::decode_bencode_object(value)?),
                (b"length", value) => length = Some(u64::decode_bencode_object(value)?),
                (b"md5sum", value) => md5sum = Some(String::decode_bencode_object(value)?),
                _ => {}
            }
        }

        if piece_length.is_none() || pieces_raw.is_none() {
            return Err(bendy::decoding::Error::missing_field(
                "piece length or pieces",
            ));
        }
        let pl = piece_length.unwrap();
        let raw = pieces_raw.unwrap();
        if raw.len() % 20 != 0 {
            return Err(bendy::decoding::Error::missing_field(
                "Invalid length for pieces",
            ));
        }
        let mut pieces = vec![];
        for chunk in raw.chunks_exact(20) {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(chunk);
            pieces.push(arr);
        }

        let mut hasher = Sha1::new();
        hasher.update(dict.into_raw()?);
        let hash = hasher.finalize().into();

        let name = name.expect("Decoding Error: Missing name from torrent info");

        if let Some(files) = files {
            Ok(Self {
                name,
                files,
                hash,
                piece_length: pl,
                pieces,
            })
        } else {
            // single-file torrent: use the name as the file path
            Ok(Self {
                name: name.clone(),
                files: vec![File {
                    length: length.expect("Decoding Error: Missing file length"),
                    md5sum,
                    path: PathBuf::from(name.clone()),
                }],
                hash,
                piece_length: pl,
                pieces,
            })
        }
    }
}

impl FromBencode for File {
    fn decode_bencode_object(object: Object) -> BendyResult<Self>
    where
        Self: Sized,
    {
        let mut length = None;
        let mut md5sum = None;
        let mut path = PathBuf::new();

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"length", value) => length = Some(u64::decode_bencode_object(value)?),
                (b"md5sum", value) => md5sum = Some(String::decode_bencode_object(value)?),
                (b"path", value) => {
                    path = Vec::decode_bencode_object(value)?
                        .into_iter()
                        .map(|bytes| String::from_utf8(bytes).unwrap())
                        .collect()
                }
                _ => {}
            }
        }

        let length = length.expect("Decoding Error: File missing length");

        Ok(Self {
            length,
            md5sum,
            path,
        })
    }
}
