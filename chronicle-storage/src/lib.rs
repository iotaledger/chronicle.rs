// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle Storage
//! This crate provides the Chronicle interface with scylla.rs.
/// Scylla access trait implementations
pub mod access;
/// Defines keyspace implementations
pub mod keyspaces;

pub use bee_tangle::ConflictReason;
