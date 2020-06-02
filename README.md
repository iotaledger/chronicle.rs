<h1 align="center">
  <br>
  <a href="https://docs.iota.org/docs/chronicle/1.0/overview"><img src="Chronicle.png"></a>
</h1>

<h2 align="center">The official IOTA permanode solution and runtime framework</h2>

<p align="center">
    <a href="https://docs.iota.org/docs/chronicle/1.0/overview" style="text-decoration:none;">
    <img src="https://img.shields.io/badge/Documentation%20portal-blue.svg?style=for-the-badge" alt="Developer documentation portal">
</p>
<p align="center">
    <a href="https://discord.iota.org/" style="text-decoration:none;"><img src="https://img.shields.io/badge/Discord-9cf.svg?logo=discord" alt="Discord"></a>
    <a href="https://iota.stackexchange.com/" style="text-decoration:none;"><img src="https://img.shields.io/badge/StackExchange-9cf.svg?logo=stackexchange" alt="StackExchange"></a>
    <a href="https://github.com/iotaledger/chronicle.rs/LICENSE" style="text-decoration:none;"><img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="Apache 2.0 license"></a>
    <a href="https://dependabot.com" style="text-decoration:none;"><img src="https://api.dependabot.com/badges/status?host=github&repo=iotaledger/chronicle.rs" alt=""></a>
</p>
      
<p align="center">
  <a href="#about">About</a> ◈
  <a href="#prerequisites">Prerequisites</a> ◈
  <a href="#installation">Installation</a> ◈
  <a href="#build-and-run">Build and Run</a> ◈
  <a href="#getting-started">Getting started</a> ◈
  <a href="#api-reference">API reference</a> ◈
  <a href="#supporting-the-project">Supporting the project</a> ◈
  <a href="#joining-the-discussion">Joining the discussion</a> 
</p>

---

## TO-DO
- Modify the links for official documentations
- Modify the Chronicle.png
- Update the current supported APIs
  - getTrytes
  - findTransactions 

## About

The Chronile is open-source Rust software that provides solution for efficient online big data storing, as well as reliable and efficient runtime framework based on [tokio](https://docs.rs/crate/tokio). This software uses IOTA protocol as an example to demonstrate

- Store transactions to a [ScyllaDB](https://www.scylladb.com/) asynchronously in real time
- Retreive transactions based on a predefined data model in efficient ways
- Allow users to interact with the database through an HTTP API
- Build and run asynchronous rust applications

Chronicle also provides a Rust asynchronous Cassandra driver based on [CQL BINARY PROTOCOL v4](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec), which allows users to encode/decode/send/retreive frames with Cassandra databses.

This is beta software, so there may be performance and stability issues.
Please report any issues in our [issue tracker](https://github.com/iotaledger/chronicle.rs/issues/new).

## Prerequisites

To run Chronicle, you need the following:

- 4GB RAM
- 64-bit processor
- [Rust](https://www.rust-lang.org/tools/install)
- [ScyllaDB](https://docs.scylladb.com/getting-started/)

## Installation

- Install [Rust](https://www.rust-lang.org/tools/install)
- Install [Docker](https://docs.docker.com/get-docker/)
- Install [ScyllaDB](https://docs.scylladb.com/getting-started/)

## Build and Run
- [Run ScyllaDB in Docker](https://docs.scylladb.com/operating-scylla/procedures/tips/best_practices_scylla_on_docker/)
- Clone and build Chronicle
```bash
git clone https://github.com/iotaledger/chronicle.rs.git
cd chronicle.rs
cargo build
```
- Go to the example folder to run the example
```bash
cd chronicle-example
cargo run --example [EXAMPLE_NAME]
```

## Getting started

For instructions on running Chronicle, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.0/tutorials/install-chronicle).

## API reference

For an API reference, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.0/references/chronicle-api-reference).

## Supporting the project

If you want to contribute to Chronicle, consider posting a [bug report](https://github.com/iotaledger/chronicle.rs/issues/new), [feature request](https://github.com/iotaledger/chronicle.rs/issues/new) or a [pull request](https://github.com/iotaledger/chronicle.rs/pulls). 

Please read the following before contributing:

- [Contributing guidelines](CONTRIBUTING.md)
- [Responsible disclosure policy](SECURITY.MD)

## Joining the discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).
