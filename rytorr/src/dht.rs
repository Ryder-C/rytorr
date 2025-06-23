use futures::StreamExt;
use mainline::{async_dht::AsyncDht, Id, Dht};
use std::net::SocketAddrV4;

/// Wrapper around the [`mainline`] DHT implementation.
#[derive(Debug)]
pub struct DhtNode {
    dht: AsyncDht,
}

impl DhtNode {
    /// Create a new DHT node in client mode and wait for bootstrapping.
    pub async fn new() -> std::io::Result<Self> {
        let dht = Dht::client()?.as_async();
        dht.bootstrapped().await;
        Ok(Self { dht })
    }

    /// Announce ourselves for the given info hash and port.
    pub async fn announce(&self, info_hash: [u8; 20], port: u16) {
        let id = Id::from_bytes(info_hash).expect("info hash has invalid length");
        let _ = self.dht.announce_peer(id, Some(port)).await;
    }

    /// Query the DHT for peers for the given info hash.
    pub async fn get_peers(&self, info_hash: [u8; 20]) -> Vec<SocketAddrV4> {
        let id = Id::from_bytes(info_hash).expect("info hash has invalid length");
        let mut stream = self.dht.get_peers(id);
        let mut result = Vec::new();
        while let Some(mut peers) = stream.next().await {
            result.append(&mut peers);
        }
        result
    }
}
