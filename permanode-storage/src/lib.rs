#![warn(missing_docs)]
//! # Permanode Storage
//! This crate provides the Permanode interface with scylla.rs.
/// Scylla access trait implementations
pub mod access;
/// Defines storage config
pub mod config;
/// Defines keyspace implementations
pub mod keyspaces;
pub mod partitioner;
pub use config::*;
pub use partitioner::Partitioner;
