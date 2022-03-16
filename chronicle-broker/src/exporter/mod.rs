// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
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
use std::ops::Range;

pub struct Exporter {
    ms_range: Range<u32>,
    message_records: Collection<MessageRecord>,
    responder: TopologyResponder,
}

impl Exporter {
    pub fn new(ms_range: Range<u32>, message_records: Collection<MessageRecord>, responder: TopologyResponder) -> Self {
        Self {
            ms_range,
            message_records,
            responder,
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
impl<S> Actor<S> for Exporter
where
    S: SupHandle<Self>,
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

            if let Some(milestone) = self
                .message_records
                .find_one(doc! {"message.payload": {"essence.index": ms}}, None)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
            {
                let mut milestone_data = MilestoneDataBuilder::new(ms, CreatedBy::Exporter);
                debug!("Found milestone message {}", milestone.message_id());
                milestone_data.set_milestone(MilestoneMessage::new(milestone));

                let mut messages = self
                    .message_records
                    .find(doc! {"milestone_index": ms}, None)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                while let Some(message) = messages.try_next().await.map_err(|e| anyhow::anyhow!(e))? {
                    milestone_data.add_message(message, None);
                }

                let milestone_data = milestone_data.build()?;
                debug!("Finished processing milestone {}", ms);
                debug!("Milestone Data:\n{:?}", milestone_data);
                archiver
                    .send(ArchiverEvent::MilestoneData(
                        milestone_data,
                        CreatedBy::Exporter,
                        Some(self.ms_range.end),
                    ))
                    .map_err(|e| anyhow!("Error sending to archiver: {}", e))?;
            } else {
                self.responder
                    .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                        "Missing milestone for index: {}!",
                        ms
                    )))))
                    .await
                    .ok();
                return Err(anyhow!("Missing milestone for index: {}!", ms).into());
            }
        }
        archiver.send(ArchiverEvent::Close(self.ms_range.end)).ok();
        self.responder
            .reply(Ok(TopologyOk::Export(ExporterStatus::Done)))
            .await
            .ok();
        Ok(())
    }
}
