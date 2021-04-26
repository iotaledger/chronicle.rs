// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! Chronicle Broker

/// The broker application
pub mod application;
/// The archiver, which stores milestones and all data in write-ahead-logs
pub mod archiver;
/// The collector, which gathers data from feeds and APIs on request
pub mod collector;
/// The importer, which enables to import write-ahead-logs
pub mod importer;
/// The listener, which receives incoming connections
pub mod listener;
/// MQTT handler
pub mod mqtt;
/// Missing data requester
pub mod requester;
/// Data solidifier
pub mod solidifier;
/// Milestone syncer
pub mod syncer;
/// Websocket command router
pub mod websocket;

#[allow(unused)]
use anyhow::{
    anyhow,
    bail,
    ensure,
};
use application::{
    BrokerChild,
    BrokerEvent,
    BrokerHandle,
    ChronicleBrokerScope,
};
use bee_common::packable::Packable;
use bee_message::{
    Message,
    MessageId,
};
use chronicle_common::{
    config::MqttType,
    get_config,
    get_config_async,
    SyncRange,
};
use chronicle_storage::access::*;
use log::*;
use paho_mqtt::{
    AsyncClient,
    CreateOptionsBuilder,
};
use scylla_rs::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
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
use url::Url;
