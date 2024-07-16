use crate::{peer::Peer, tracker::MAX_PEERS};

use super::{Trackable, TrackerResponse};
use anyhow::{bail, Result};
use bendy::decoding::FromBencode;
use urlencoding::encode_binary;

// Tracker parameters from bittorrent spec <https://wiki.theory.org/BitTorrentSpecification#Tracker_HTTP.2FHTTPS_Protocol>
pub struct Http<'a> {
    url: String,
    info_hash: String,
    peer_id: String,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    size: u64,
    // Compact always 1
    event: Option<&'a str>,
    tracker_id: Option<String>,
}

#[derive(Debug)]
pub struct HttpResponse {
    pub failure_reason: Option<String>,
    pub warning_message: Option<String>,
    pub interval: Option<u32>,
    pub _min_interval: Option<u32>,
    pub tracker_id: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<Peer>,
}

impl<'a> Http<'a> {
    pub fn new(url: String, info_hash: &'static [u8], peer_id: String, port: u16, size: u64) -> Self {
        let event = None; // Some(EVENT_STARTED);
        let info_hash = encode_binary(info_hash).into_owned();

        Self {
            url,
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            size,
            event,
            tracker_id: None,
        }
    }
}

impl<'a> Trackable for Http<'a> {
    fn scrape(&mut self) -> Result<TrackerResponse> {
        // Have to do this to avoid info_hash being double url encoded. Cant find a way to pass in bytes or disable url encoding in ureq.
        let request_url = format!("{}?info_hash={}", &self.url, &self.info_hash);

        let mut request = ureq::get(&request_url)
            .query("peer_id", &self.peer_id)
            .query("port", &self.port.to_string())
            .query("uploaded", &self.uploaded.to_string())
            .query("downloaded", &self.downloaded.to_string())
            .query("left", &(self.size - self.downloaded).to_string())
            .query("compact", "1")
            .query("numwant", &MAX_PEERS.to_string());

        if let Some(event) = self.event {
            request = request.query("event", event);
            self.event = None;
        }

        if let Some(tracker_id) = &self.tracker_id {
            request = request.query("trackerid", tracker_id);
        }

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
            println!("Tracker Warning: {}", message);
        }

        self.tracker_id.clone_from(&response.tracker_id);

        response.try_into()
    }

    fn update_progress(&mut self, downloaded: u64, uploaded: u64) {
        self.downloaded = downloaded;
        self.uploaded = uploaded;
    }
}
