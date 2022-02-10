---
image: /img/chronicle_icon.png
description:  Configure your Chronicle node keyspaces, number of threads, cores, milestones, data sync.  
keywords:
- scylla cluster
- number of thread
- number of core
- number of milestones
- mqtt topic
- data sync range
- reference
---
# Config Reference

## storage_config

### keyspaces: Vec&lt;KeyspaceConfig>

You can configure multiple keyspaces to filter incoming messages. If you are not using the `filter` feature, *only the
first configured keyspace will be considered* or the default (`chronicle`) if none is provided.

In addition to the keyspace name, each keyspace requires a map of datacenters (name -> replication factor). You can find
more configurations about datacenters in ScyllaDB in
this [Scylla University course](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/datacenter/)
.

### listen_address: String

The scylla.rs dashboard listening address, where it accepts requests to manage the Scylla cluster.

### thread_count: Enum

The number of threads Scylla will use. Can be one of `Count(usize)` (a simple scalar count) or `CoreMultiple(usize)` (a
multiple of the number of cores the system has).

### reporter_count: u8

The number of reporters Scylla will spawn.

### local_datacenter: String

The Scylla local datacenter.

### partition_config

Specifies the number of partitions to use in the database, as well as the number of milestones to use as chunks.

:::note notice

You won't be able to change the `partition_config` without a migration.

:::

## api_config

The API can only be configured using the [.env](https://github.com/iotaledger/chronicle.rs/blob/main/.env) file at the
moment.

## broker_config

### websocket_address: String

The Broker dashboard listening address, where it accepts requests to manage the broker topology.

### mqtt_brokers: Vec&lt;Url>

- Messages: mqtt topic used to receive incoming IOTA messages.
- MessagesReferenced: mqtt topic used to receive incoming metadata.

:::note notice

You should have one of each at least.

:::

### api_endpoints: Vec&lt;Url>

IOTA node-endpoints that will be used by chronicle to fill gaps.

### retries_per_endpoint: u8

Max number of retries to retrieve something from `api_endpoints`.

### retries_per_query: usize

Max number of retries to fetch/insert something from/to `scylla` before declaring an outage, which will force the broker
application to pause and wait for scylla cluster to recover.

### collector_count: u8

The number of concurrent collectors which collect data from feed sources. It is also used as `solidifier_count`.

### requester_count: u8

The number of concurrent requesters per collector.

:::note notice

Requesters are used by the collector to fetch missing data from `api_endpoint`.

:::

### request_timeout_secs: u64

The `api_endpoint` request timeout.

### parallelism: u8

The max number of concurrent solidify requests.

### sync_range: Option&lt;SyncRange>

Identify the milestone data sync range from/to.

### complete_gaps_interval_secs: u64

Interval used by the syncer to check if there are some gaps to fill/complete.

### logs_dir: Option&lt;String>

If provided, it will archive the milestone data in an ordered fashion.

### max_log_size: Option&lt;u64>

The upper limit of the log_file_size.

:::note

Ensure you use a limit within your filesystem range.

:::

