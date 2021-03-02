#![warn(missing_docs)]
//! # Permanode Storage
//! This crate provides the Permanode interface with scylla.rs.
/// Scylla access trait implementations
pub mod access;
/// Defines storage config
pub mod config;
/// Defines keyspace implementations
pub mod keyspaces;

pub use config::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
