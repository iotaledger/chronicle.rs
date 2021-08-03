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
/// The importer, which enables importing write-ahead-logs
#[cfg(feature = "application")]
pub mod importer;

/// The exporter, which enables exporting write-ahead-logs
#[cfg(feature = "application")]
pub mod exporter;
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
#[cfg(feature = "application")]
mod app {
    use super::*;
    pub use anyhow::{
        anyhow,
        bail,
        ensure,
    };
    pub use application::BrokerEvent;
    pub use async_trait::async_trait;
    pub use backstage::prelude::*;
    pub use bee_common::packable::Packable;
    pub use bee_message::{
        Message,
        MessageId,
    };
    pub use chronicle_common::{
        config::MqttType,
        get_config_async,
        get_history_mut_async,
        SyncRange,
    };
    pub use chronicle_storage::access::*;
    pub use log::*;
    pub use paho_mqtt::{
        AsyncClient,
        CreateOptionsBuilder,
    };
    pub use scylla_rs::prelude::*;
    pub use serde::{
        Deserialize,
        Serialize,
    };
    pub use std::{
        collections::HashMap,
        convert::{
            TryFrom,
            TryInto,
        },
        marker::PhantomData,
        ops::{
            Deref,
            DerefMut,
        },
        path::PathBuf,
    };
    pub use url::Url;
}
#[cfg(feature = "application")]
use app::*;

#[cfg(feature = "merge")]
/// Provide the archive file merger functionality;
pub mod merge;

mod types;
pub use types::*;
