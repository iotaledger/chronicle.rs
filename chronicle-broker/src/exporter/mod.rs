// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{
    filter::FilterHandle,
    *,
};
use crate::{
    application::{
        TopologyOk,
        TopologyResponder,
    },
    archiver::{
        ArchiverEvent,
        ArchiverHandle,
    },
};
use anyhow::anyhow;
use backstage::core::{
    Actor,
    ActorResult,
    NullChannel,
    Rt,
    SupHandle,
};
use chronicle_common::types::CreatedBy;
use std::ops::Range;

pub struct Exporter<T: FilterBuilder> {
    ms_range: Range<u32>,
    responder: TopologyResponder,
    filter_handle: T::Handle,
}

impl<T: FilterBuilder> Exporter<T> {
    pub fn new(ms_range: Range<u32>, responder: TopologyResponder, filter_handle: T::Handle) -> Self {
        Self {
            ms_range,
            responder,
            filter_handle,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ExporterStatus {
    InProgress { current: u32, completed: u32, total: u32 },
    Done,
    Failed(String),
}

#[async_trait]
impl<S, T> Actor<S> for Exporter<T>
where
    S: SupHandle<Self>,
    T: FilterBuilder,
{
    type Data = ArchiverHandle;
    type Channel = NullChannel;

    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        Ok(rt
            .lookup(
                rt.sibling("archiver")
                    .scope_id()
                    .await
                    .ok_or_else(|| anyhow!("No archiver!"))?,
            )
            .await
            .ok_or_else(|| anyhow!("No archiver!"))?)
    }

    async fn run(&mut self, rt: &mut Rt<Self, S>, archiver: Self::Data) -> ActorResult<()> {
        for ms in self.ms_range.clone() {
            debug!("Processing milestone {}", ms);
            self.responder
                .reply(Ok(TopologyOk::Export(ExporterStatus::InProgress {
                    current: ms,
                    completed: ms - self.ms_range.start,
                    total: self.ms_range.end - self.ms_range.start,
                })))
                .await
                .ok();
            if let Ok(Some(milestone_data)) = self.filter_handle.export_milestone_data(ms).await {
                archiver
                    .send(ArchiverEvent::MilestoneData(
                        milestone_data,
                        CreatedBy::Exporter,
                        Some(self.ms_range.end),
                    ))
                    .map_err(|e| {
                        anyhow!(
                            "Error sending exported milestone data for index: {}, to archiver: {}",
                            ms,
                            e
                        )
                    })?;
            } else {
                self.responder
                    .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                        "Missing milestone data for index: {}!",
                        ms
                    )))))
                    .await
                    .ok();
                archiver.send(ArchiverEvent::Close(ms)).ok();
                return Err(anyhow!("Unable to export milestone data for index: {}!", ms).into());
            };
        }
        self.responder
            .reply(Ok(TopologyOk::Export(ExporterStatus::Done)))
            .await
            .ok();
        Ok(())
    }
}
