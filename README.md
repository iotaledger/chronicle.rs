<h1 align="center">
  <br>
  <a href="https://docs.iota.org/docs/permanode/1.1/overview"><img src=".github/Chronicle.png"></a>
</h1>

<h2 align="center">A Permanent IOTA Message Storage Solution</h2>

<p align="center">
    <a href="https://docs.iota.org/docs/chronicle/1.1/overview" style="text-decoration:none;">
    <img src="https://img.shields.io/badge/Documentation%20portal-blue.svg?style=for-the-badge" alt="Developer documentation portal">
</p>
<p align="center">
    <a href="https://github.com/iotaledger/permanode.rs/actions" style="text-decoration:none;"><img src="https://github.com/iotaledger/permanode.rs/workflows/Build/badge.svg"></a>
    <a href="https://github.com/iotaledger/permanode.rs/actions" style="text-decoration:none;"><img src="https://github.com/iotaledger/permanode.rs/workflows/Test/badge.svg"></a>
    <a href="https://discord.iota.org/" style="text-decoration:none;"><img src="https://img.shields.io/badge/Discord-9cf.svg?logo=discord" alt="Discord"></a>
    <a href="https://iota.stackexchange.com/" style="text-decoration:none;"><img src="https://img.shields.io/badge/StackExchange-9cf.svg?logo=stackexchange" alt="StackExchange"></a>
    <a href="https://github.com/iotaledger/permanode.rs/blob/master/LICENSE" style="text-decoration:none;"><img src="https://img.shields.io/badge/License-Apache%202.0-green.svg" alt="Apache 2.0 license"></a>
    <a href="https://dependabot.com" style="text-decoration:none;"><img src="https://api.dependabot.com/badges/status?host=github&repo=iotaledger/permanode.rs" alt=""></a>
</p>

<p align="center">
  <a href="#about">About</a> ◈
  <a href="#prerequisites">Prerequisites</a> ◈
  <a href="#api-reference">API Reference</a> ◈
  <a href="#config-reference">Config Reference</a> ◈
  <a href="#supporting-the-project">Supporting the project</a> ◈
  <a href="#joining-the-discussion">Joining the discussion</a> ◈
  <a href="#future-work">Future work</a> ◈
  <a href="#LICENSE">LICENSE</a>
</p>

---

## About

Chronicle provides tools for managing and accessing permanode solutions using the IOTA actor framework [backstage](#). With Chronicle, you can:

- Store IOTA messages in real time, using one or more [Scylla](https://www.scylladb.com/) clusters
- Explore stored messages using an HTTP API
- Store the data you want by modifying incoming messages
- Filter data to store it how and where you want

Chronicle includes the following crates:

- **[Chronicle](permanode/README.md)** The entry point for the Chronicle application
- **[API](permanode-api/README.md):** API that allows you to access stored messages
- **[Broker](permanode-broker/README.md):** Allows subscribing to incoming messages from IOTA nodes
- **[Storage](permanode-storage/README.md):** Implements storage related functionality from [scylla.rs](https://github.com/iotaledger/scylla.rs)
- **[Filter](permanode-filter/README.md):** A simple container for user modified code

**Note:** This is alpha software, so there may be performance and stability issues. Please report any issues in our [issue tracker](https://github.com/iotaledger/permanode.rs/issues/new).

## Prerequisites

To run Chronicle, you need the following:

- A Linux LTS operating system such as [Ubuntu](https://ubuntu.com/download#download)

- 4 GB RAM

- At least 32 GB of disk space

- 64-bit processor

- Preferred a 10 Gbps network connection

- At least 2 CPU cores (only for the meantime)

    You can check how many CPU cores your device has, using the following command:

    ```bash
    nproc
    ```

- [Rust](https://www.rust-lang.org/tools/install)

- At least one Scylla node (version 4 or greater) running on a different device in the same private network as Chronicle. See the [Scylla documentation](https://docs.scylladb.com/getting-started/) for a tutorial on setting one up. For information about securing your Scylla nodes, see the [Scylla security documentation](https://docs.scylladb.com/operating-scylla/security/).

- The `build-essentials` packages

    You can install these packages for Debian based distros, using the following command:

    ```bash
    sudo apt-get install build-essential gcc make cmake cmake-gui cmake-curses-gui
    ```
    For other Linux distros, please refer to your package manager to install the build-essential pkgs



- (Optional) An IDE that supports Rust autocompletion. We recommend [Visual Studio Code](https://code.visualstudio.com/Download) with the [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=matklad.rust-analyzer) extension

- If you want to load historical transactions into your permanode, you can download the files from the [IOTA Foundation's archive](https://dbfiles.iota.org/?prefix=mainnet/history/).

We also recommend updating Rust to the latest stable version:

```bash
rustup update stable
```

## Installation

Either download the provided executable (you should only do this if you do not wish to use the filtering functionality), or build it yourself.

### Building Chronicle

Clone this repository:

```bash
git clone https://github.com/iotaledger/chronicle.rs
```

If you wish to use the filter functionality, enable the `filter` feature in [chronicle](chronicle/Cargo.toml)

```bash
cargo build --release --features filter
```

### Configuring Chronicle

Chronicle uses a [RON](https://github.com/ron-rs/ron) file to store configuration parameters, called `config.ron`. An example is provided as [config.example.ron](config.example.ron) with default values. See <a href="#config-reference">Config Reference</a> for more details about the config file.

## API Reference

For an API reference, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.1/references/chronicle-api-reference).

## Config Reference

### `storage_config`

#### `keyspaces: Vec<KeyspaceConfig>`
See [KeyspaceConfig](permanode-storage/src/config.rs#KeyspaceConfig)

Multiple keyspaces can be configured in order to filter incoming messages. If the `filter` feature is not used, *only the first configured keyspace will be considered* or the default (`chronicle`) if none is provided.

In addition to the keyspace name, each requires a map of datacenters (name -> replication factor). See [here](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/datacenter/) for more information about datacenters in ScyllaDB.

#### `listen_address: String`
The scylla.rs dashboard listen address, where it accepts requests to manage the Scylla cluster.

#### `thread_count: Enum`
The number of threads Scylla will use. Can be one of `Count(usize)` (a simple scalar count) or `CoreMultiple(usize)` (a multiple of the number of cores the system has).

#### `reporter_count: u8`
The number of reporters Scylla will spawn.

#### `local_datacenter: String`
The Scylla local datacenter.

#### `partition_config`
See [PartitionConfig](permanode-storage/src/config.rs#PartitionConfig)

Specifies the number of partitions to use in the database, as well as the number of milestones to use as chunks.

### `api_config`

TODO

### `broker_config`

#### `mqtt_brokers: Vec<Url>`
TODO

#### `endpoints: Vec<Url>`
TODO

## Supporting the project

If you want to contribute to Chronicle, consider posting a [bug report](https://github.com/iotaledger/chronicle.rs/issues/new?template=bug-report-for-chronicle.md), [feature request](https://github.com/iotaledger/chronicle.rs/issues/new?template=feature-request-for-chronicle.md) or a [pull request](https://github.com/iotaledger/chronicle.rs/pulls).

Please read the following before contributing:

- [Contributing guidelines](.github/CONTRIBUTING.md)

## Joining the discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).

## Future work

- Add more examples and documentation
- Add more test cases
- Add more field tests
- Allow Chronicle to solidify transactions
- Implement selective permanode
- Add dashboard web app
- Improve framework

## LICENSE

(c) 2021 - IOTA Stiftung

IOTA Chronicle is distributed under the Apache License (Version 2.0).
