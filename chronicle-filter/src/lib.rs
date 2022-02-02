// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use backstage::core::{
    Actor,
    Channel,
    SupHandle,
};
use bee_message::{
    Message,
    MessageId,
};
use chronicle_storage::access::{
    MessageMetadata,
    MilestoneData,
    Selected,
};

use serde::{
    Deserialize,
    Serialize,
};
use std::fmt::Debug;
use wildmatch::WildMatch;

#[async_trait::async_trait]

pub trait SelectiveBuilder<S: SupHandle<Self::Actor>>:
    'static + Debug + PartialEq + Eq + Sized + Send + Clone + Serialize + Sync + std::default::Default
{
    type Actor: Actor<S>;
    async fn build(&self) -> anyhow::Result<(Self::Actor, <Self::Actor as Actor<S>>::Channel)>;
    async fn filter_message(
        &self,
        handle: &<<Self::Actor as Actor<S>>::Channel as Channel>::Handle,
        message_id: &MessageId,
        message: &Message,
        metadata: Option<&MessageMetadata>,
    ) -> anyhow::Result<Option<Selected>>;
    async fn process_milestone_data(
        &self,
        handle: &<<Self::Actor as Actor<S>>::Channel as Channel>::Handle,
        milestone_data: std::sync::Arc<MilestoneData>,
    ) -> anyhow::Result<()>;
}

// NOTE the selective impl not complete
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SelectivePermanodeConfig {
    // indexation_keys: Vec<u8>,
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
