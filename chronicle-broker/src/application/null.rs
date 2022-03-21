// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::filter::{
    FilterBuilder,
    FilterHandle,
};
use async_trait::async_trait;

use backstage::core::{
    AbortableUnboundedChannel,
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    SupHandle,
};
use chronicle_common::types::{
    MessageRecord,
    MilestoneData,
    MilestoneDataBuilder,
    MilestoneDataSearch,
    Selected,
    SyncData,
};

use futures::stream::StreamExt;
use std::ops::Range;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct NullConfig;

#[derive(Debug, Clone)]
pub struct NullFilterHandle;

#[async_trait]
impl FilterBuilder for NullConfig {
    type Actor = Uda;
    type Handle = NullFilterHandle;
    async fn build(&self) -> ActorResult<Self::Actor> {
        Ok(Uda)
    }
    async fn handle(&self, _handle: AbortableUnboundedHandle<()>) -> ActorResult<Self::Handle> {
        Ok(NullFilterHandle)
    }
}

#[async_trait]
impl FilterHandle for NullFilterHandle {
    async fn filter_message(&self, message: &MessageRecord) -> ActorResult<Option<Selected>> {
        Ok(None)
    }
    async fn export_milestone_data(&self, milestone_index: u32) -> ActorResult<Option<MilestoneData>> {
        Ok(None)
    }
    async fn process_milestone_data(&self, milestone_data: MilestoneDataBuilder) -> ActorResult<MilestoneData> {
        if !milestone_data.valid() {
            return Err(ActorError::aborted_msg(
                "Cannot process milestone data using invalid milestone data builder",
            ))?;
        }
        // convert it to searchable struct
        let milestone = milestone_data.milestone().clone().unwrap();
        // Convert milestone data builder
        let mut milestone_data_search: MilestoneDataSearch = milestone_data.try_into()?;
        let mut milestone_data = MilestoneData::new(milestone);
        while let Some(message) = milestone_data_search.next().await {
            // add it to milestone data
            milestone_data.messages.insert(message);
        }
        Ok(milestone_data)
    }
    async fn synced(&self, milestone_index: u32) -> ActorResult<()> {
        Ok(())
    }
    async fn logged(&self, milestone_index: u32) -> ActorResult<()> {
        Ok(())
    }
    async fn sync_data(&self, range: Option<Range<u32>>) -> ActorResult<SyncData> {
        Ok(SyncData::default())
    }
}

/// User defined actor
pub struct Uda;

/// The Lifecycle of user defined actor
#[async_trait]
impl<S> Actor<S> for Uda
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = AbortableUnboundedChannel<()>;

    async fn init(&mut self, _rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<()> {
        while let Some(_) = rt.inbox_mut().next().await {
            break;
        }
        Ok(())
    }
}
