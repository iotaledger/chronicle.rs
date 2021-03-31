#![warn(missing_docs)]
//! Common code for Chronicle

use serde::{
    Deserialize,
    Serialize,
};

/// Configuration for the Chronicle application
pub mod config;
pub mod metrics;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct SyncRange {
    pub from: u32,
    pub to: u32,
}
impl Default for SyncRange {
    fn default() -> Self {
        Self {
            from: 1,
            to: i32::MAX as u32,
        }
    }
}
#[derive(Clone, Copy)]
pub struct Synckey;
