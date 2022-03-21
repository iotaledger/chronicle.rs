// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! Common code for Chronicle

use lazy_static::lazy_static;
use serde::{
    Deserialize,
    Serialize,
};
use std::ops::Range;

pub mod alert;
/// Configuration for the Chronicle application
pub mod config;
/// Metrics for prometheus integration
pub mod metrics;

/// Common chronicle types
pub mod types;

/// Pub send_alert
pub use alert::send_alert;

pub mod cpt2 {
    pub use bee_message_cpt2::{
        self,
        *,
    };
    pub use bee_rest_api_cpt2::{
        self,
        *,
    };
}
pub mod shimmer {
    pub use bee_message_shimmer::{
        self,
        *,
    };
    pub use bee_rest_api_shimmer::{
        self,
        *,
    };
}

/// Defines a range of milestone indexes to be synced
// todo impl checked deserialize which verify the range
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct SyncRange {
    /// First milestone index (inclusive)
    pub from: u32,
    /// Last milestone index (exclusive)
    pub to: u32,
}

impl std::convert::TryFrom<Range<u32>> for SyncRange {
    type Error = anyhow::Error;
    fn try_from(r: Range<u32>) -> Result<Self, Self::Error> {
        Self::try_new(r.start, r.end)
    }
}

#[allow(missing_docs)]
impl SyncRange {
    pub fn try_new(from: u32, to: u32) -> anyhow::Result<Self> {
        if from == 0 || to == 0 {
            anyhow::bail!("Error verifying sync from/to, zero provided!\nPlease provide non-zero milestone index");
        } else if from >= to {
            anyhow::bail!("Error verifying sync from/to, greater or equal provided!\nPlease provide lower \"Sync range from\" milestone index");
        }
        Ok(SyncRange { from, to })
    }
    pub fn start(&self) -> &u32 {
        &self.from
    }

    pub fn end(&self) -> &u32 {
        &self.to
    }
}

impl Default for SyncRange {
    fn default() -> Self {
        Self {
            from: 1,
            to: i32::MAX as u32,
        }
    }
}

/// Defines a wrapper type which wraps a dereference-able inner value
pub trait Wrapper: std::ops::Deref {
    /// Consume the wrapper and retrieve the inner type
    fn into_inner(self) -> Self::Target;
}
