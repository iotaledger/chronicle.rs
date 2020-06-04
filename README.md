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
    <a href="https://github.com/iotaledger/chronicle.rs/blob/master/LICENSE" style="text-decoration:none;"><img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="Apache 2.0 license"></a>
    <a href="https://dependabot.com" style="text-decoration:none;"><img src="https://api.dependabot.com/badges/status?host=github&repo=iotaledger/chronicle.rs" alt=""></a>
</p>
      
<p align="center">
  <a href="#about">About</a> ◈
  <a href="#prerequisites">Prerequisites</a> ◈
  <a href="#installation">Installation</a> ◈
  <a href="#build-and-run">Build and Run</a> ◈
  <a href="#api-reference">API reference</a> ◈
  <a href="#supporting-the-project">Supporting the project</a> ◈
  <a href="#joining-the-discussion">Joining the discussion</a> 
</p>

---

## About

The Chronile is an open-source Rust framework that provides solution for efficient online big data access with databases, as well as reliable and efficient runtime framework based on [tokio](https://docs.rs/crate/tokio). This framework uses IOTA protocol as an example to demonstrate

- Store transactions to a [ScyllaDB](https://www.scylladb.com/) asynchronously in real time
- Get transactions based on a predefined data model in efficient ways
- Allow users to interact with the database through an HTTP API
- Build and run asynchronous rust applications

Chronicle also provides a Rust asynchronous Cassandra driver based on [CQL BINARY PROTOCOL v4](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec), which allows users to encode/decode frames with Cassandra databases. The details of other crates in Chronicle frame are as follows

- [Chronicle API](chronicle-api/README.md)
- [Chronicle Broker](chronicle-broker/README.md)
- [Chronicle Common](chronicle-common/README.md)
- [Chronicle CQL](chronicle-cql/README.md)
- [Chronicle Storage](chronicle-storage/README.md)

[Examples](examples/README.md) of using Chronicle framework is provided.

This is alpha software, so there may be performance and stability issues.
Please report any issues in our [issue tracker](https://github.com/iotaledger/chronicle.rs/issues/new).

## Prerequisites

To run Chronicle, you need the following:

- 4GB RAM
- 64-bit processor
- [Rust](https://www.rust-lang.org/tools/install)

To use the storage crate, you need:

- [ScyllaDB](https://docs.scylladb.com/getting-started/) or other [Cassandra](https://cassandra.apache.org/) databases to store and access historical transactions

## Installation

- Install [Rust](https://www.rust-lang.org/tools/install)

To use the storage crate, you need:

- Install [ScyllaDB](https://docs.scylladb.com/getting-started/) or other Cassandra databases
- Install [Docker](https://docs.docker.com/get-docker/) or other  containers for the database

## Build and run

- [Run ScyllaDB in Docker](https://docs.scylladb.com/operating-scylla/procedures/tips/best_practices_scylla_on_docker/) for quick usage of storage crate
- Clone and build Chronicle
```bash
git clone https://github.com/iotaledger/chronicle.rs.git
cd chronicle.rs
cargo build
```
- Go to the example folder to run the example
```bash
cd examples
cargo run --example [EXAMPLE_NAME]
```
- The historical dmp files can be downloaded [here](https://dbfiles.iota.org/?prefix=mainnet/history/)

For instructions on running Chronicle, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.0/tutorials/install-chronicle).

## API reference

For an API reference, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.0/references/chronicle-api-reference).

## Supporting the project

If you want to contribute to Chronicle, consider posting a [bug report](https://github.com/iotaledger/chronicle.rs/issues/new), [feature request](https://github.com/iotaledger/chronicle.rs/issues/new) or a [pull request](https://github.com/iotaledger/chronicle.rs/pulls).

Please read the following before contributing:

- [Contributing guidelines](CONTRIBUTING.md)

## Joining the discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).
