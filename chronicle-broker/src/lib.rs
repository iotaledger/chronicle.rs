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
/// The importer, which enables to import write-ahead-logs
#[cfg(feature = "application")]
pub mod importer;
/// The listener, which receives incoming connections
#[cfg(feature = "application")]
pub mod listener;
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
/// Websocket command router
#[cfg(feature = "application")]
pub mod websocket;
#[cfg(feature = "application")]
mod app {
    use super::*;
    pub use anyhow::{
        anyhow,
        bail,
        ensure,
    };
    pub use application::{
        BrokerChild,
        BrokerEvent,
        BrokerHandle,
        ChronicleBrokerScope,
    };
    pub use bee_common::packable::Packable;
    pub use bee_message::{
        Message,
        MessageId,
    };
    pub use chronicle_common::{
        config::MqttType,
        get_config,
        get_config_async,
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

mod types;
pub use types::*;
