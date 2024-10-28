use crate::{peer::Peer, tracker::MAX_PEERS};

use super::{Trackable, TrackerResponse};
use anyhow::{bail, Result};
use bendy::decoding::{FromBencode, Object, ResultExt as _};
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

impl<'a> Http<'a> {
    pub fn new(
        url: String,
        info_hash: &'static [u8],
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Self {
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

impl FromBencode for HttpResponse {
    fn decode_bencode_object(
        object: bendy::decoding::Object,
    ) -> std::result::Result<Self, bendy::decoding::Error>
    where
        Self: Sized,
    {
        let mut failure_reason = None;
        let mut warning_message = None;
        let mut interval = None;
        let mut min_interval = None;
        let mut tracker_id = None;
        let mut seeders = None;
        let mut leechers = None;
        let mut peers = vec![];

        let mut dict = object
            .try_into_dictionary()
            .context("response bytes to dictionary")?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"failure reason", value) => {
                    failure_reason = String::decode_bencode_object(value)
                        .context("failure reason")
                        .map(Some)?
                }
                (b"warning message", value) => {
                    warning_message = String::decode_bencode_object(value)
                        .context("warning message")
                        .map(Some)?
                }
                (b"interval", value) => {
                    interval = u32::decode_bencode_object(value)
                        .context("interval")
                        .map(Some)?
                }
                (b"min interval", value) => {
                    min_interval = u32::decode_bencode_object(value)
                        .context("min interval")
                        .map(Some)?
                }
                (b"tracker id", value) => {
                    tracker_id = String::decode_bencode_object(value)
                        .context("tracker id")
                        .map(Some)?
                }
                (b"complete", value) => {
                    seeders = u32::decode_bencode_object(value)
                        .context("seeders")
                        .map(Some)?
                }
                (b"incomplete", value) => {
                    leechers = u32::decode_bencode_object(value)
                        .context("leechers")
                        .map(Some)?
                }
                (b"peers", value) => {
                    // Peers can come as bencoded List of Dictionaries OR stream of bytes
                    match value {
                        Object::List(mut raw_peer_list) => {
                            while let Some(raw_peer) = raw_peer_list.next_object()? {
                                let peer = Peer::decode_bencode_object(raw_peer)
                                    .context("peer dictionary")?;
                                peers.push(peer);
                            }
                        }
                        Object::Bytes(raw_peer_bytes) => {
                            for raw_peer in raw_peer_bytes.chunks(6) {
                                peers.push(match Peer::from_be_bytes(raw_peer) {
                                    Ok(peer) => peer,
                                    Err(_) => continue,
                                });
                            }
                        }
                        _ => {
                            return Err(bendy::decoding::Error::missing_field(
                                "Object::Dict or Object::Bytes",
                            ))
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            failure_reason,
            warning_message,
            interval,
            _min_interval: min_interval,
            tracker_id,
            seeders,
            leechers,
            peers,
        })
    }
}
