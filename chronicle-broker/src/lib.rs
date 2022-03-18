// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! Chronicle Broker
/// The broker application
#[cfg(feature = "application")]
pub mod application;
/// The archiver, which stores milestones and all data in write-ahead-logs
#[cfg(feature = "application")]
pub mod archiver;
/// The collector, which gathers data from feeds and APIs on request
#[cfg(feature = "application")]
pub mod collector;
/// Exporter
#[cfg(feature = "application")]
pub mod exporter;
/// The importer, which enables to import write-ahead-logs
#[cfg(feature = "application")]
pub mod importer;
/// MQTT handler
#[cfg(feature = "application")]
pub mod mqtt;
/// Missing data requester
#[cfg(feature = "application")]
pub mod requester;
/// Data solidifier
#[cfg(feature = "application")]
pub mod solidifier;
/// Milestone syncer
#[cfg(feature = "application")]
pub mod syncer;

/// filter module
#[cfg(feature = "application")]
pub mod filter;

#[cfg(feature = "application")]
use application::BrokerHandle;

#[cfg(feature = "application")]
use filter::{
    FilterBuilder,
    FilterHandle,
};

#[cfg(feature = "application")]
mod app {
    pub use anyhow::{
        anyhow,
        bail,
        ensure,
    };
    pub use chronicle_common::SyncRange;
    pub use log::*;
    pub use serde::{
        Deserialize,
        Serialize,
    };
    pub use std::{
        collections::{
            BinaryHeap,
            HashMap,
        },
        convert::{
            TryFrom,
            TryInto,
        },
        ops::{
            Deref,
            DerefMut,
        },
        path::PathBuf,
    };
    pub use url::Url;
}

#[cfg(feature = "application")]
pub(crate) use app::*;

#[cfg(feature = "merge")]
/// Provide the archive file merger functionality;
pub mod merge;

#[cfg(feature = "application")]
use async_trait::async_trait;
