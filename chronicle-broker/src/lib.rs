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

pin_project! {
    #[must_use = "futures/streams do nothing unless you poll them"]
    pub struct MilestoneDataSearch {
        #[pin]
        data: MilestoneDataBuilder,
        #[pin]
        should_be_visited: VecDeque<Proof>,
        #[pin]
        visited: HashSet<MessageId>,
        budget: usize,
        counter: usize,
    }
}

impl std::convert::TryFrom<MilestoneDataBuilder> for MilestoneDataSearch {
    type Error = anyhow::Error;

    fn try_from(data: MilestoneDataBuilder) -> Result<Self, Self::Error> {
        if !data.valid() {
            anyhow::bail!("cannot make milestone data search struct for uncompleted milestone data")
        }
        let milestone_message_id = data
            .milestone()
            .as_ref()
            .unwrap() // unwrap is safe, as this right after valid check.
            .message()
            .message_id();
        let mut should_be_visited = VecDeque::new();
        // we start from the root
        should_be_visited.push_back(Proof::new(data.milestone_index(), vec![*milestone_message_id]));
        Ok(Self {
            data,
            should_be_visited,
            visited: Default::default(),
            budget: 128,
            counter: 0,
        })
    }
}

impl futures::stream::Stream for MilestoneDataSearch {
    type Item = (Option<Proof>, MessageRecord);

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut project = self.as_mut().project();
        if project.counter == project.budget {
            *project.counter = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        while let Some(current_proof) = project.should_be_visited.pop_front() {
            *project.counter += 1;
            let message_id = *current_proof.path().last().expect("The path should never be empty");
            project.visited.insert(message_id);
            // iterate over its parents
            if let Some(message) = project.data.messages().get(&message_id) {
                let parents_iter = message.parents().iter();
                for parent_id in parents_iter {
                    if !project.visited.contains(parent_id) {
                        let mut vertex = current_proof.clone();
                        vertex.path_mut().push(parent_id.clone());
                        project.should_be_visited.push_back(vertex);
                    }
                }
                // check if this message is selected
                if project.data.selected_messages().contains_key(&message_id) {
                    return Poll::Ready(Some((Some(current_proof), message.clone())));
                } else {
                    return Poll::Ready(Some((None, message.clone())));
                }
            } else {
                // reached the end of the branch, proceed to the next should_be_visited
                continue;
            }
        }
        Poll::Ready(None)
    }
}

#[async_trait::async_trait]

pub trait SelectiveBuilder<S: SupHandle<Self::Actor>>:
    'static + Debug + PartialEq + Eq + Sized + Send + Clone + Serialize + Sync + std::default::Default
{
    type Actor: Actor<S>;
    async fn build(&self) -> anyhow::Result<(Self::Actor, <Self::Actor as Actor<S>>::Channel)>;
    async fn filter_message(
        &self,
        handle: &<<Self::Actor as Actor<S>>::Channel as Channel>::Handle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>>;
    async fn process_milestone_data(
        &self,
        handle: &<<Self::Actor as Actor<S>>::Channel as Channel>::Handle,
        milestone_data: std::sync::Arc<MilestoneDataBuilder>,
    ) -> anyhow::Result<()>;
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
    async fn process_milestone_data_builder(
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
