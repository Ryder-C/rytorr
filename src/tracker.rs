use anyhow::{bail, Context, Result};
use http::HttpResponse;
use std::fmt;

use crate::swarm::Peer;

pub mod http;
pub mod udp;

const EVENT_STARTED: &str = "Started";
const EVENT_STOPPED: &str = "Stopped";
const EVENT_COMPLETED: &str = "Completed";

#[derive(Debug)]
pub struct UnrecognizedTrackerError;

impl std::error::Error for UnrecognizedTrackerError {}

impl fmt::Display for UnrecognizedTrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unrecognized tracker protocol")
    }
}

pub enum TrackerType {
    HTTP,
    UDP,
}

impl TrackerType {
    pub fn type_from_url(url: &str) -> Result<Self, UnrecognizedTrackerError> {
        if url.starts_with("http") {
            Ok(Self::HTTP)
        } else if url.starts_with("udp") {
            Ok(Self::UDP)
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
    pub interval: u64,
    tracker_id: String,
    leechers: u32,
    seeders: u32,
    peers: Vec<Peer>,
}

impl TryFrom<HttpResponse> for TrackerResponse {
    type Error = anyhow::Error;

    fn try_from(value: HttpResponse) -> std::result::Result<Self, Self::Error> {
        let interval = value.interval.context("interval not found")?;
        let tracker_id = value.tracker_id.context("tracker_id not found")?;
        let leechers = value.leechers.context("leechers not found")?;
        let seeders = value.seeders.context("seeders not found")?;
        let peers = value.peers;

        Ok(Self {
            interval,
            tracker_id,
            leechers,
            seeders,
            peers,
        })
    }
}
