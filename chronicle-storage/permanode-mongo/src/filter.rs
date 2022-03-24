// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use chronicle_broker::filter::{
    FilterBuilder,
    FilterHandle,
};

use backstage::core::{
    AbortableUnboundedChannel,
    AbortableUnboundedHandle,
    Actor,
    ActorError,
    ActorResult,
    Rt,
    SupHandle,
};
use chronicle_common::{
    config::mongo::MongoConfig,
    mongodb::{
        options::ClientOptions,
        Client,
    },
    types::{
        MessageRecord,
        MilestoneData,
        MilestoneDataBuilder,
        MilestoneDataSearch,
        Selected,
        SyncData,
    },
};
use futures::stream::StreamExt;
use std::{
    ops::Range,
    time::Duration,
};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
/// The permanode mongo config
pub struct PermanodeMongoConfig {
    database_name: String,
    mongo_config: MongoConfig,
}

#[derive(Debug, Clone)]
/// Permanode mongo filter handle
pub struct PermanodeMongoHandle {
    database: mongodb::Database,
}

#[async_trait]
impl FilterBuilder for PermanodeMongoConfig {
    type Actor = Uda;
    type Handle = PermanodeMongoHandle;
    async fn build(&self) -> ActorResult<Self::Actor> {
        Ok(Uda)
    }
    async fn handle(&self, _handle: AbortableUnboundedHandle<()>) -> ActorResult<Self::Handle> {
        let client_opts: ClientOptions = self.mongo_config.clone().into();
        let database = mongodb::Client::with_options(client_opts)
            .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?
            .database("permanode");
        Ok(PermanodeMongoHandle { database })
    }
}

#[async_trait]
impl FilterHandle for PermanodeMongoHandle {
    async fn filter_message(&self, _message: &MessageRecord) -> ActorResult<Option<Selected>> {
        Ok(Some(Selected::select()))
    }
    async fn export_milestone_data(&self, _milestone_index: u32) -> ActorResult<Option<MilestoneData>> {
        Ok(None)
    }
    async fn process_milestone_data(&self, milestone_data: MilestoneDataBuilder) -> ActorResult<Option<MilestoneData>> {
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
            // insert into mongo
            self.database
                .collection::<MessageRecord>("messages")
                .insert_one(&message, None)
                .await
                .map_err(|e| ActorError::restart(e, Some(Duration::from_secs(360))))?;
            // add it to milestone data
            milestone_data.messages.insert(message);
        }
        Ok(Some(milestone_data))
    }
    async fn synced(&self, _milestone_index: u32) -> ActorResult<()> {
        // todo insert sync record for this milestone
        Ok(())
    }
    async fn logged(&self, _milestone_index: u32) -> ActorResult<()> {
        // todo insert sync logged record for this milestone
        Ok(())
    }
    async fn sync_data(&self, _range: Option<Range<u32>>) -> ActorResult<SyncData> {
        // todo retrieve sync record for the provided range
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
