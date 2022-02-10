---
image: /img/chronicle_icon.png
description: Chronicle provides tools for managing and accessing permanode solutions using the IOTA actor framework backstage to allow you to store, explore and filer IOTA messages.
keywords:
- permanode
- store messages
- explore messages
- filter messages
- project structure
- explanation
---
# Welcome

Chronicle provides tools for managing and accessing permanode solutions using the IOTA actor framework [backstage](https://github.com/iotaledger/backstage) that allow you to:

- Store IOTA messages in real time, using one or more [Scylla](https://www.scylladb.com/) clusters.
- Explore stored messages using an HTTP API.
- Store the data you want by modifying incoming messages.
- Filter data to store it how and where you want (work in progress).

:::note

This is alpha software, so there may be performance and stability issues. Please report any issues in our [issue tracker](https://github.com/iotaledger/chronicle.rs/issues/new).

:::

## Project Structure

Chronicle includes the following crates:

### [Chronicle](https://github.com/iotaledger/chronicle.rs/tree/main/chronicle)

The entry point for the Chronicle application.

### [API](https://github.com/iotaledger/chronicle.rs/tree/main/chronicle-api)

API that allows you to access stored messages.

### [Broker](https://github.com/iotaledger/chronicle.rs/tree/main/chronicle-broker)

Allow you to subscribe to incoming messages from IOTA nodes.

### [Storage](https://github.com/iotaledger/chronicle.rs/tree/main/chronicle-storage)

Implements storage related functionality from [scylla.rs](https://github.com/iotaledger/scylla.rs).

### [Filter](https://github.com/iotaledger/chronicle.rs/tree/main/chronicle-filter)

A simple container for user modified code.

## Join the Discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).