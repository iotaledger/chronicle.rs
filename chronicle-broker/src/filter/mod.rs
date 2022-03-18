use super::application::BrokerHandle;
use backstage::core::{
    Actor,
    ActorResult,
    Channel,
    ChannelBuilder,
};
use bee_message::milestone::MilestoneIndex;

use chronicle_common::types::*;
use serde::Serialize;
use std::{
    fmt::Debug,
    ops::Range,
};

#[async_trait::async_trait]
pub trait FilterBuilder:
    'static + Debug + PartialEq + Eq + Sized + Send + Clone + Serialize + Sync + std::default::Default
{
    type Actor: Actor<BrokerHandle> + ChannelBuilder<<Self::Actor as Actor<BrokerHandle>>::Channel>;
    type Handle: FilterHandle;
    const NAME: &'static str = "uda";
    async fn build(&self) -> ActorResult<Self::Actor>;
    async fn handle(
        &self,
        handle: <<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
    ) -> ActorResult<Self::Handle>;
}

#[async_trait::async_trait]
pub trait FilterHandle: Clone + Send + 'static + Sync {
    /// Filter the message, used by Collector(s)
    async fn filter_message(&self, message: &MessageRecord) -> ActorResult<Option<Selected>>;
    /// Process milestone data, used by Solidifier(s) and Importer(s)
    async fn process_milestone_data(&self, milestone_data: MilestoneDataBuilder) -> ActorResult<MilestoneData>;
    /// Export milestone data, used by Exporter
    async fn export_milestone_data(&self, milestone_index: u32) -> ActorResult<Option<MilestoneData>>;
    /// Inserts Sync record highlighting a synced milestone data, used by solidifier(s) and Importer(s)
    async fn synced(&self, milestone_index: u32) -> ActorResult<()>;
    /// Inserts Sync record highlighting a logged milestone data, used by Archiver and Importer(s)
    async fn logged(&self, milestone_index: u32) -> ActorResult<()>;
    /// Gets the sync data information
    async fn sync_data(&self, range: Option<Range<u32>>) -> ActorResult<SyncData>;
}
