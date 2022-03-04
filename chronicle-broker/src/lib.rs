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

mod inserts;
use application::{
    permanode::Uda,
    BrokerHandle,
};
use filter::{
    AtomicProcessHandle,
    FilterBuilder,
};

#[cfg(feature = "application")]
mod app {
    pub use anyhow::{
        anyhow,
        bail,
        ensure,
    };
    pub use bee_message::{
        Message,
        MessageId,
    };
    pub use chronicle_common::SyncRange;
    pub use chronicle_storage::access::*;
    pub use log::*;
    pub use packable::{
        Packable,
        PackableExt,
    };
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
use std::{
    collections::{
        HashSet,
        VecDeque,
    },
    fmt::Debug,
    task::{
        Context,
        Poll,
    },
};

#[cfg(feature = "application")]
pub(crate) use app::*;

#[cfg(feature = "merge")]
/// Provide the archive file merger functionality;
pub mod merge;
use async_trait::async_trait;
use pin_project_lite::pin_project;
use wildmatch::WildMatch;

/// The inherent trait to return a boxed worker for a given key/value pair in a keyspace
pub(crate) trait Inherent<S, K, V> {
    type Output: Worker;
    fn inherent_boxed(&self, keyspace: S, key: K, value: V) -> Box<Self::Output>
    where
        S: 'static + Insert<K, V> + Debug,
        K: 'static + Send + Sync + Clone + Debug + TokenEncoder,
        V: 'static + Send + Sync + Clone + Debug;
}

// NOTE the selective impl not complete
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SelectivePermanodeConfig {
    // indexation_keys: Vec<u8>,
}

#[async_trait]
impl FilterBuilder for SelectivePermanodeConfig {
    type Actor = Uda;

    async fn build(&self) -> anyhow::Result<(Self::Actor, <Self::Actor as Actor<BrokerHandle>>::Channel)> {
        todo!()
    }
    async fn filter_message(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>> {
        todo!()
    }
    async fn process_milestone_data(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData> {
        todo!()
    }
}

#[derive(Clone, Debug, Default, Copy)]
pub struct SelectivePermanode;

// todo impl deserialize and serialize
pub enum IndexationKey {
    Text(WildMatch),
    Hex(WildMatch),
}

struct HexedIndex {
    /// The hexed index with wildcard
    hexed_index: WildMatch,
}

impl HexedIndex {
    fn new(hexed_index: WildMatch) -> Self {
        Self { hexed_index }
    }
}
