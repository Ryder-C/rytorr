use anyhow::{bail, Context, Result};
use http::HttpResponse;
use std::fmt;
use udp::Udp;

use crate::swarm::Peer;

pub mod http;
pub mod udp;

const EVENT_STARTED: &str = "Started";
const EVENT_STOPPED: &str = "Stopped";
const EVENT_COMPLETED: &str = "Completed";

const MAX_PEERS: usize = 50;

#[derive(Debug)]
pub struct UnrecognizedTrackerError;

impl std::error::Error for UnrecognizedTrackerError {}

impl fmt::Display for UnrecognizedTrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unrecognized tracker protocol")
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

pub trait Trackable {
    fn scrape(&mut self) -> Result<TrackerResponse>;
}

#[derive(Debug)]
pub struct TrackerResponse {
    pub interval: u32,
    leechers: Option<u32>,
    seeders: Option<u32>,
    peers: Vec<Peer>,
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
