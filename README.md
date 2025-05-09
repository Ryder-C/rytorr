# rytorr

A BitTorrent client written in Rust.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](
https://github.com/Ryder-C/rytorr)

![GIF](https://github.com/Ryder-C/rytorr/blob/main/test_run.gif)

## Usage

```bash
rytorr <torrent_file_path>
```

## Features

* [x] Parsing of .torrent files
* [x] Communication with trackers (HTTP and UDP)
* [x] Peer-to-peer communication (TCP)
* [x] Implementation of core BitTorrent protocol messages (handshake, bitfield, have, piece, request, etc.)
* [x] Downloading and saving torrent data to disk
* [x] Basic swarm management (peer connections, choking/unchoking)

## Planned Features

* [ ] BitTorrent v2 support
* [ ] DHT (Distributed Hash Table) support
* [ ] Magnet link support
* [ ] Peer exchange (PEX)
* [ ] Local Peer Discovery (LPD)
* [ ] uTP (Micro Transport Protocol)
* [ ] CLI Tool
* [ ] Seeding capabilities
* [ ] Selective file downloading
* [ ] Dynamic pipelining (request blocks depending on peers observed bandwith delay product)
