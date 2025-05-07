# rytorr

A BitTorrent client written in Rust.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](
https://github.com/Ryder-C/rytorr)

![GIF](https://github.com/Ryder-C/rytorr/blob/main/test_run.gif)

## Features

* Parsing of .torrent files
* Communication with trackers (HTTP and UDP)
* Peer-to-peer communication (TCP)
* Implementation of core BitTorrent protocol messages (handshake, bitfield, have, piece, request, etc.)
* Downloading and saving torrent data to disk
* Basic swarm management (peer connections, choking/unchoking)

## Planned Features

* [ ] BitTorrent v2 support
* [ ] DHT (Distributed Hash Table) support
* [ ] Magnet link support
* [ ] Peer exchange (PEX)
* [ ] Local Peer Discovery (LPD)
* [ ] uTP (Micro Transport Protocol)
* [ ] Web UI
* [ ] Seeding capabilities
* [ ] Selective file downloading
