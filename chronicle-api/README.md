<h2 align="center">Chronicle-API Crate</h2>

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
  <a href="#api-calls">API calls</a> ◈
  <a href="#supporting-the-project">Supporting the project</a> ◈
  <a href="#joining-the-discussion">Joining the discussion</a> 
</p>

---

## About

Chronicle-API crate is associated with Chronicle framework. It implements the functionalities of [APIs](https://docs.iota.org/docs/chronicle/1.0/references/chronicle-api-reference) those can be used for chronicle.

## API calls

- **getTrytes**
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
- **findTransactions**
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
- **findTransactions**
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
- **findTransactions**
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
- **findTransactions**
```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"hints": [
  {"address":"ADDRESS_1","month":8,"year":2019}
  ]
}'
```
- **findTransactions**
```bash
curl http://host:port/api
-X POST
-H 'Content-Type: application/json'
-H 'X-IOTA-API-Version: 1'
-d '{
"command": "findTransactions",
"hints": [
  {"tag":"TAG_1","month":8,"year":2019}
  ]
}'
```

## Supporting the project

If you want to contribute to Chronicle, consider posting a [bug report](https://github.com/iotaledger/chronicle.rs/issues/new), [feature request](https://github.com/iotaledger/chronicle.rs/issues/new) or a [pull request](https://github.com/iotaledger/chronicle.rs/pulls).

Please read the following before contributing:

- [Contributing guidelines](CONTRIBUTING.md)

## Joining the discussion

If you want to get involved in the community, need help with getting set up, have any issues related to Chronicle, or just want to discuss IOTA, Distributed Registry Technology (DRT) and IoT with other people, feel free to join our [Discord](https://discord.iota.org/).
