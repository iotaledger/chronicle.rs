<h2 align="center">API</h2>

<p align="center">
    <a href="https://docs.iota.org/docs/chronicle/1.1/overview" style="text-decoration:none;">
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
  <a href="#api-endpoints">API endpoints</a> ◈
  <a href="#supporting-the-project">Supporting the project</a> ◈
  <a href="#joining-the-discussion">Joining the discussion</a>
</p>

---

## About

This crate implements an HTTP API that you can use to access transactions in a database that is connected to Chronicle.

For an example of how to use this crate, see the [`broker` example](https://github.com/iotaledger/chronicle.rs/blob/982bf8d8206d5d7e36589d37407fb8884485e51c/examples/broker/main.rs#L33).

## API endpoints

This crate implements the following endpoints. For details, see the [documentation portal](https://docs.iota.org/docs/chronicle/1.1/references/chronicle-api-reference).


- **getTrytes** by transaction hashes

```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "getTrytes",
"hashes": [
  "TRANSACTION_HASH_1","TRANSACTION_HASH_N"
  ]
}'
```
- **findTransactions** by bundle hashes

```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"bundles": [
  "BUNDLE_HASH_1", "BUNDLE_HASH_N"
  ]
}'
```
- **findTransactions** by approvees

```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"approvees": [
  "HASH_1", "HASH_N"
  ]
}'
```
- **findTransactions** by addresses

```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"addresses": [
  "ADDRESS_1","ADDRESS_N"
  ]
}'
```
- **findTransactions** by tags

```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"tags": [
  "TAG_1", "TAG_N"
  ]
}'
```
- **findTransactions** by hints

All what the users have to do is to reflect the received hints with the next call till the response returns no hints at all.

## Find Transactions Changes
- **findTransactions** returns `values/milestones/timestamps` beside `hashes`
- **findTransactions** by `addresses/bundles/approvees/tags` might return `hints` which indicates the need for further calls to fetch the remaining pages

## Supporting the project

If you want to contribute to Chronicle, consider posting a [bug report](https://github.com/iotaledger/chronicle.rs/issues/new), [feature request](https://github.com/iotaledger/chronicle.rs/issues/new) or a [pull request](https://github.com/iotaledger/chronicle.rs/pulls).

Please read the following before contributing:

- [Contributing guidelines](.github/CONTRIBUTING.md)

## Joining the discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).
