use crate::{peer::Peer, tracker::MAX_PEERS};
use std::sync::Arc;

use super::{Trackable, TrackerResponse};
use anyhow::{bail, Result};
use bendy::decoding::{FromBencode, Object, ResultExt as _};
use tracing::{debug, error, instrument, trace, warn};
use urlencoding::encode_binary;

// Tracker parameters from bittorrent spec <https://wiki.theory.org/BitTorrentSpecification#Tracker_HTTP.2FHTTPS_Protocol>
#[derive(Debug)]
pub struct Http {
    url: String,
    info_hash: Arc<Vec<u8>>,
    peer_id: String,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    left: u64,
    size: u64,
    // Compact always 1
    tracker_id: Option<String>,

    // State for event reporting
    started_sent: bool,
    needs_complete_event: bool,
}

impl Http {
    #[instrument(skip(info_hash, peer_id), fields(url = %url, port = port, size = size))]
    pub fn new(
        url: String,
        info_hash: Arc<Vec<u8>>,
        peer_id: String,
        port: u16,
        size: u64,
    ) -> Self {
        debug!("Creating new HTTP tracker instance");
        Self {
            url,
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left: size,
            size,
            tracker_id: None,
            started_sent: false,
            needs_complete_event: false,
        }
    }
}

impl Trackable for Http {
    #[instrument(name="http_scrape", skip(self), fields(url = %self.url, tracker_id = ?self.tracker_id))]
    fn scrape(&mut self) -> Result<TrackerResponse> {
        trace!("Starting HTTP scrape");
        let event_to_send = if !self.started_sent {
            Some("started")
        } else if self.needs_complete_event {
            Some("completed")
        } else {
            None
        };
        debug!(event = ?event_to_send, downloaded = self.downloaded, uploaded = self.uploaded, left = self.left, "Determined scrape parameters");

        let info_hash_encoded = encode_binary(&self.info_hash).into_owned();
        let request_url = format!("{}?info_hash={}", &self.url, &info_hash_encoded);

        let mut request_builder = ureq::get(&request_url)
            .query("peer_id", &self.peer_id)
            .query("port", &self.port.to_string())
            .query("uploaded", &self.uploaded.to_string())
            .query("downloaded", &self.downloaded.to_string())
            .query("left", &self.left.to_string())
            .query("compact", "1")
            .query("numwant", &MAX_PEERS.to_string());

        if let Some(event) = event_to_send {
            request_builder = request_builder.query("event", event);
        }

        if let Some(tracker_id) = &self.tracker_id {
            request_builder = request_builder.query("trackerid", tracker_id);
        }

        debug!(final_url = %request_builder.url(), "Sending tracker request");

        let mut response_reader = match request_builder.call() {
            Ok(resp) => resp.into_reader(),
            Err(e) => {
                error!(error = %e, "HTTP tracker request failed");
                bail!("HTTP tracker request failed: {}", e);
            }
        };

        let mut bytes = vec![];
        if let Err(e) = response_reader.read_to_end(&mut bytes) {
            error!(error = %e, "Failed to read tracker response body");
            bail!("Failed to read tracker response body: {}", e);
        }
        debug!(
            response_len = bytes.len(),
            "Received tracker response bytes"
        );

        let response = match HttpResponse::from_bencode(&bytes) {
            Ok(resp) => resp,
            Err(e) => {
                error!(error = %e, "Failed to parse tracker response Bencode");
                bail!("Invalid Bencoded Response from Tracker: {}", e);
            }
        };
        trace!(?response, "Parsed tracker response");

        if let Some(reason) = &response.failure_reason {
            error!(failure_reason = %reason, "Tracker reported failure");
            bail!("Tracker Failed: {}", reason)
        }

        if let Some(message) = &response.warning_message {
            warn!(warning_message = %message, "Tracker Warning");
        }

        if self.tracker_id != response.tracker_id {
            debug!(old_tracker_id = ?self.tracker_id, new_tracker_id = ?response.tracker_id, "Updating tracker ID");
            self.tracker_id = response.tracker_id.clone();
        } else {
            trace!("Tracker ID unchanged");
        }

        if event_to_send.is_some() {
            if event_to_send == Some("started") {
                trace!("Marking 'started' event as sent");
                self.started_sent = true;
            }
            if event_to_send == Some("completed") {
                trace!("Resetting 'needs_complete_event' flag");
                self.needs_complete_event = false;
            }
        }

        match TrackerResponse::try_from(response) {
            Ok(parsed_resp) => {
                debug!(
                    peers_count = parsed_resp.peers.len(),
                    interval = parsed_resp.interval,
                    "Scrape successful"
                );
                Ok(parsed_resp)
            }
            Err(e) => {
                error!(error = %e, "Failed to convert internal HttpResponse to TrackerResponse");
                bail!("Failed to process tracker response fields: {}", e)
            }
        }
    }

    #[instrument(level = "trace", skip(self), fields(url = %self.url))]
    fn update_progress(&mut self, downloaded: u64, uploaded: u64, left: u64) {
        if left == 0 && self.left != 0 {
            debug!("Download completed, scheduling 'completed' event");
            self.needs_complete_event = true;
        }
        self.downloaded = downloaded;
        self.uploaded = uploaded;
        self.left = left;
        trace!(downloaded, uploaded, left, "Updated tracker progress");
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

#[cfg(test)]
mod tests {
    use super::*;
    use bendy::decoding::FromBencode;

    #[test]
    fn test_http_response_list_peers() {
        // keys sorted: complete, incomplete, interval, peers; inner peer dict sorted: ip, port
        let bencoded =
            b"d8:completei2e10:incompletei3e8:intervali60e5:peersld2:ip9:127.0.0.14:porti6881eeee";
        let resp = HttpResponse::from_bencode(&bencoded[..]).unwrap();
        assert_eq!(resp.interval.unwrap(), 60);
        assert_eq!(resp.seeders.unwrap(), 2);
        assert_eq!(resp.leechers.unwrap(), 3);
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].ip, "127.0.0.1");
        assert_eq!(resp.peers[0].port, 6881);
    }

    #[test]
    fn test_http_response_compact_peers() {
        let peers_bytes = [127, 0, 0, 1, 0x1A, 0xE1];
        // keys sorted: complete, incomplete, interval, peers; peers value is bytes with length prefix
        let mut bencoded = b"d8:completei1e10:incompletei1e8:intervali30e5:peers6:".to_vec();
        bencoded.extend(&peers_bytes);
        bencoded.extend(b"e"); // close dict
        let resp = HttpResponse::from_bencode(&bencoded[..]).unwrap();
        assert_eq!(resp.interval.unwrap(), 30);
        assert_eq!(resp.seeders.unwrap(), 1);
        assert_eq!(resp.leechers.unwrap(), 1);
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].ip, "127.0.0.1");
        assert_eq!(resp.peers[0].port, 6881);
    }
}
