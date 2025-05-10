use anyhow::{bail, Context, Result};
use http::HttpResponse;
use std::fmt::{self, Display};

use crate::peer::Peer;

pub mod http;
pub mod udp;

const MAX_PEERS: usize = 50;

#[derive(Debug)]
pub struct UnrecognizedTrackerError;

impl std::error::Error for UnrecognizedTrackerError {}

impl fmt::Display for UnrecognizedTrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unrecognized tracker protocol")
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Started,
    Stopped,
    Completed,
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Started => "Started",
                Self::Stopped => "Stopped",
                Self::Completed => "Completed",
            }
        )
    }
}

pub enum TrackerType {
    Http,
    Udp,
}

impl TrackerType {
    pub fn type_from_url(url: &str) -> Result<Self, UnrecognizedTrackerError> {
        if url.starts_with("http") {
            Ok(Self::Http)
        } else if url.starts_with("udp") {
            Ok(Self::Udp)
        } else {
            Err(UnrecognizedTrackerError)
        }
    }
}

pub trait Trackable: Send + Sync {
    fn update_progress(&mut self, downloaded: u64, uploaded: u64, left: u64);
    fn scrape(&mut self) -> Result<TrackerResponse>;
}

#[derive(Debug)]
pub struct TrackerResponse {
    pub interval: u32,
    pub leechers: Option<u32>,
    pub seeders: Option<u32>,
    pub peers: Vec<Peer>,
}

impl TrackerResponse {
    pub fn from_udp_response(response: &[u8], transaction_id: u32) -> Result<Self> {
        if transaction_id != u32::from_be_bytes(response[4..8].try_into()?) {
            bail!("Transaction ID Mismatch")
        }

        if 1 != u32::from_be_bytes(response[0..4].try_into()?) {
            bail!("Recieved Action not Announce")
        }

        let interval = u32::from_be_bytes(response[8..12].try_into()?);
        let leechers = Some(u32::from_be_bytes(response[12..16].try_into()?));
        let seeders = Some(u32::from_be_bytes(response[16..20].try_into()?));
        let peers = response[20..]
            .chunks(6)
            .map(Peer::from_be_bytes)
            .filter_map(|x| x.ok())
            .collect();

        Ok(Self {
            interval,
            leechers,
            seeders,
            peers,
        })
    }
}

impl TryFrom<HttpResponse> for TrackerResponse {
    type Error = anyhow::Error;

    fn try_from(value: HttpResponse) -> std::result::Result<Self, Self::Error> {
        let interval = value.interval.context("interval not found")?;
        let leechers = value.leechers;
        let seeders = value.seeders;
        let peers = value.peers;

        Ok(Self {
            interval,
            leechers,
            seeders,
            peers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_display() {
        assert_eq!(Event::Started.to_string(), "Started");
        assert_eq!(Event::Stopped.to_string(), "Stopped");
        assert_eq!(Event::Completed.to_string(), "Completed");
    }

    #[test]
    fn test_type_from_url() {
        assert!(matches!(
            TrackerType::type_from_url("http://example.com"),
            Ok(TrackerType::Http)
        ));
        assert!(matches!(
            TrackerType::type_from_url("udp://tracker:8080"),
            Ok(TrackerType::Udp)
        ));
        assert!(TrackerType::type_from_url("ftp://example.com").is_err());
    }

    #[test]
    fn test_from_udp_response_valid() {
        // construct valid response: action=1, transaction=42, interval=30, leechers=5, seeders=10, one peer 127.0.0.1:6881
        let mut buf = Vec::new();
        buf.extend(&1u32.to_be_bytes()); // action
        buf.extend(&42u32.to_be_bytes()); // transaction
        buf.extend(&30u32.to_be_bytes()); // interval
        buf.extend(&5u32.to_be_bytes()); // leechers
        buf.extend(&10u32.to_be_bytes()); // seeders
        buf.extend(&[127, 0, 0, 1, 0x1A, 0xE1]); // peer
        let resp = TrackerResponse::from_udp_response(&buf, 42).unwrap();
        assert_eq!(resp.interval, 30);
        assert_eq!(resp.leechers, Some(5));
        assert_eq!(resp.seeders, Some(10));
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(resp.peers[0].ip, "127.0.0.1");
        assert_eq!(resp.peers[0].port, 6881);
    }

    #[test]
    fn test_from_udp_response_errors() {
        // wrong transaction id
        let mut buf = Vec::new();
        buf.extend(&1u32.to_be_bytes());
        buf.extend(&1u32.to_be_bytes());
        buf.extend(&0u32.to_be_bytes());
        buf.extend(&0u32.to_be_bytes());
        buf.extend(&0u32.to_be_bytes());
        assert!(TrackerResponse::from_udp_response(&buf, 2).is_err());
        // wrong action
        let mut buf2 = Vec::new();
        buf2.extend(&2u32.to_be_bytes());
        buf2.extend(&0u32.to_be_bytes());
        buf2.extend(&0u32.to_be_bytes());
        buf2.extend(&0u32.to_be_bytes());
        buf2.extend(&0u32.to_be_bytes());
        assert!(TrackerResponse::from_udp_response(&buf2, 0).is_err());
    }
}
