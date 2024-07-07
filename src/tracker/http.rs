use crate::swarm::Peer;

use super::{Trackable, TrackerResponse, EVENT_STARTED};
use anyhow::{bail, Result};
use bendy::decoding::{FromBencode, ResultExt};
use urlencoding::encode_binary;

// Tracker parameters from bittorrent spec <https://wiki.theory.org/BitTorrentSpecification#Tracker_HTTP.2FHTTPS_Protocol>
pub struct Http<'a> {
    url: &'a str,
    info_hash: String,
    peer_id: &'a str,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    // Compact always 1
    event: Option<&'a str>,
}

pub struct HttpResponse {
    pub failure_reason: Option<String>,
    pub warning_message: Option<String>,
    pub interval: Option<u64>,
    pub min_interval: Option<u32>,
    pub tracker_id: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<Peer>,
}

impl<'a> Http<'a> {
    pub fn new(url: &'a str, info_hash: &[u8], peer_id: &'a str, port: u16) -> Self {
        let event = Some(EVENT_STARTED);
        let info_hash = encode_binary(info_hash).into_owned();

        Self {
            url,
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            event,
        }
    }
}

impl<'a> Trackable for Http<'a> {
    fn scrape(&mut self) -> Result<TrackerResponse> {
        // Have to do this to avoid info_hash being double url encoded. Cant find a way to pass in bytes or disable url encoding in ureq.
        let request_url = format!("{}?info_hash={}", self.url, &self.info_hash);

        let request = ureq::get(&request_url)
            // .query("info_hash", &self.info_hash)
            .query("peer_id", self.peer_id)
            .query("port", &self.port.to_string())
            .query("uploaded", &self.uploaded.to_string())
            .query("downloaded", &self.downloaded.to_string())
            .query("left", &self.left.to_string())
            .query("compact", "1");

        println!("Requesting... {:?}", request);

        // if let Some(event) = self.event {
        //     request = request.query("event", event);
        //     self.event = None;
        // }

        let mut bytes = vec![];
        request.call()?.into_reader().read_to_end(&mut bytes)?;

        let response = HttpResponse::from_bencode(&bytes);
        let response = match response {
            Ok(resp) => resp,
            Err(e) => bail!("Invalid Response from Tracker {}", e),
        };

        if let Some(reason) = &response.failure_reason {
            bail!("Tracker Failed: {}", reason)
        }

        if let Some(message) = &response.warning_message {
            println!("Tracker Warning: {:?}", message);
        }

        response.try_into()
    }
}
