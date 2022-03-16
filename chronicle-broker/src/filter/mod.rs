use super::application::BrokerHandle;
use backstage::core::{
    Actor,
    Channel,
};
use chronicle_storage::access::{
    MessageRecord,
    MilestoneData,
    MilestoneDataBuilder,
    Selected,
};
use std::fmt::Debug;
#[async_trait::async_trait]

pub trait FilterBuilder: 'static + Debug + PartialEq + Sized + Send + Clone + Sync + std::default::Default {
    type Actor: Actor<BrokerHandle>;
    async fn build(&self) -> anyhow::Result<(Self::Actor, <Self::Actor as Actor<BrokerHandle>>::Channel)>;
    async fn filter_message(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>>;
    async fn process_milestone_data(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData>;
    // todo helper methods
}
