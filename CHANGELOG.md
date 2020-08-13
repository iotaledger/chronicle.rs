# Changelog

## [0.2.0-alpha] - 2020-08-13
### Added
- Extended paging support to everything.
- IRI API compatibility.
- MQTT support.
- Support all IOTA networks (`mainnet`, `devnet`, `comnet`) using shared Scylla cluster.

### Changed
- Improved Data model.
- Simplified hints design.
- Enhanced `findTransactions` response designed specifically to enable seamless explorer service, which will return `values/milestones/timestamps` beside `hashes`.

### Removed
- Removed ZMQ support.

[0.2.0-alpha]: https://github.com/iotaledger/chronicle.rs/tree/v0.2.0-alpha
