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
    failure_reason: Option<String>,
    warning_message: Option<String>,
    pub interval: Option<u64>,
    min_interval: Option<u32>,
    pub tracker_id: Option<String>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<Peer>,
}

impl<'a> Http<'a> {
    pub fn new(url: &'a str, info_hash: &[u8], peer_id: &'a str, port: u16) -> Self {
        let event = Some(EVENT_STARTED);
        let info_hash = encode_binary(info_hash).into_owned();
        // let info_hash = url_encode_bytes(info_hash).unwrap();

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
        let mut request = ureq::get(self.url)
            .query("info_hash", &self.info_hash)
            .query("peer_id", &self.peer_id)
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
                    interval = u64::decode_bencode_object(value)
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
                    let mut raw_peer_list = value.try_into_list()?;

                    while let Some(raw_peer) = raw_peer_list.next_object()? {
                        let peer = Peer::decode_bencode_object(raw_peer).context("peer")?;
                        peers.push(peer);
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            failure_reason,
            warning_message,
            interval,
            min_interval,
            tracker_id,
            seeders,
            leechers,
            peers,
        })
    }
}
