use std::{collections::HashSet, fs, path::PathBuf};

use bendy::decoding::{FromBencode, Object, ResultExt};
use chrono::{DateTime, Local, TimeZone};
use sha1::{Digest, Sha1};

use crate::{peer::Peer, tracker::http::HttpResponse};

type BendyResult<T> = Result<T, bendy::decoding::Error>;

#[derive(Debug)]
pub struct Torrent {
    pub info: Info,
    pub announce_list: Vec<String>,
    creation_date: Option<DateTime<Local>>,
    comment: Option<String>,
    created_by: Option<String>,
    encoding: Option<String>,
}

#[derive(Debug, Default)]
pub struct Info {
    name: String,
    pub files: Vec<File>,
    pub hash: [u8; 20],
}

#[derive(Debug)]
pub struct File {
    pub length: u64,
    md5sum: Option<String>,
    path: PathBuf,
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

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"name", value) => name = Some(String::decode_bencode_object(value)?),
                (b"files", value) => files = Some(Vec::decode_bencode_object(value)?),
                (b"length", value) => length = Some(u64::decode_bencode_object(value)?),
                (b"md5sum", value) => md5sum = Some(String::decode_bencode_object(value)?),
                _ => {}
            }
        }

        let mut hasher = Sha1::new();
        hasher.update(dict.into_raw()?);
        let hash = hasher.finalize().into();

        let name = name.expect("Decoding Erorr: Missing name from torrent info");

        if let Some(files) = files {
            Ok(Self { name, files, hash })
        } else {
            Ok(Self {
                name,
                files: vec![File {
                    length: length.expect("Decoding Error: Missing file length"),
                    md5sum,
                    path: PathBuf::new(),
                }],
                hash,
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
