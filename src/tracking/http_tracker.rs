use anyhow::{bail, Result};
use bendy::decoding::FromBencode;
use std::fmt::Write;

use crate::tracking::{Peer, Trackable};
use async_trait::async_trait;

pub struct HTTP {
    pub url: String,
    pub info_hash: [u8; 20],
    pub peer_id: String,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: String,
}

#[derive(Debug)]
pub struct Response {
    pub failure_reason: Option<String>,
    pub interval: Option<u32>,
    pub tracker_id: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Option<Vec<Peer>>,
}

impl HTTP {
    pub fn new(
        url: String,
        info_hash: [u8; 20],
        peer_id: String,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: String,
    ) -> Self {
        Self {
            url,
            info_hash,
            peer_id,
            port,
            uploaded,
            downloaded,
            left,
            event,
        }
    }

    fn build_query(&self) -> ureq::Request {
        ureq::get(&self.url)
            .query("peer_id", &self.peer_id)
            .query("port", &self.port.to_string())
            .query("uploaded", &self.uploaded.to_string())
            .query("downloaded", &self.downloaded.to_string())
            .query("left", &self.left.to_string())
            .query("compact", "1")
            .query("event", &self.event)
    }
}

#[async_trait]
impl Trackable for HTTP {
    async fn announce(&mut self) -> Result<(u32, Vec<Peer>)> {
        self.request().await
    }

    async fn request(&self) -> Result<(u32, Vec<Peer>)> {
        let url_builder = self.build_query();
        let url = format!(
            "{}&info_hash={}",
            url_builder.url(),
            url_encode_bytes(&self.info_hash)?
        );

        let mut bytes = vec![];
        ureq::get(&url)
            .call()?
            .into_reader()
            .read_to_end(&mut bytes)?;

        let resp = Response::from_bencode(&bytes).unwrap();
        if let Some(failure_reason) = resp.failure_reason {
            bail!(failure_reason);
        }

        println!("{:?}", resp);

        Ok((resp.interval.unwrap(), resp.peers.unwrap()))
    }

    fn get_info_hash(&self) -> [u8; 20] {
        self.info_hash
    }

    fn get_peer_id(&self) -> String {
        self.peer_id.clone()
    }

    fn set_uploaded(&mut self, uploaded: u64) {
        self.uploaded = uploaded;
    }

    fn set_downloaded(&mut self, downloaded: u64) {
        self.downloaded = downloaded;
    }

    fn set_event(&mut self, event: String) {
        self.event = event;
    }
}

fn url_encode_bytes(bytes: &[u8]) -> Result<String> {
    let mut out: String = String::new();

    for byte in bytes.iter() {
        match *byte as char {
            '0'..='9' | 'a'..='z' | 'A'..='Z' | '.' | '-' | '_' | '~' => out.push(*byte as char),
            _ => write!(&mut out, "%{:02X}", byte)?,
        };
    }

    Ok(out)
}
