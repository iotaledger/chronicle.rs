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
use bee_message::milestone::{
    Milestone,
    MilestoneIndex,
};
use std::ops::Range;

pub struct Exporter {
    ms_range: Range<u32>,
    keyspace: ChronicleKeyspace,
    responder: TopologyResponder,
}

impl Exporter {
    pub fn new(ms_range: Range<u32>, keyspace: ChronicleKeyspace, responder: TopologyResponder) -> Self {
        Self {
            ms_range,
            keyspace,
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

            if let Some(milestone) =
                query::<Bee<Milestone>, _, _>(&self.keyspace, &Bee(MilestoneIndex::from(ms)), None, None).await?
            {
                let ms_message_id = *milestone.message_id();
                let mut milestone_data = MilestoneDataBuilder::new(ms, CreatedBy::Exporter);
                debug!("Found milestone data {}", milestone.message_id());
                let mut paging_state = None;
                let milestone =
                    match query::<MessageRecord, _, _>(&self.keyspace, &Bee(ms_message_id), None, None).await? {
                        Some(m) => m,
                        None => {
                            self.responder
                                .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                                    "Missing milestone message {}!",
                                    ms
                                )))))
                                .await
                                .ok();
                            return Err(anyhow!("Missing milestone message {}!", ms).into());
                        }
                    };
                debug!("Found milestone message {}", milestone.message_id());
                milestone_data.set_milestone(MilestoneMessage { message: milestone });
                loop {
                    let messages = match query::<Paged<Iter<MessageRecord>>, _, _>(
                        &self.keyspace,
                        &Bee(ms_message_id),
                        None,
                        paging_state,
                    )
                    .await?
                    {
                        Some(m) => m,
                        None => {
                            self.responder
                                .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                                    "Missing messages for milestone {}!",
                                    ms
                                )))))
                                .await
                                .ok();
                            return Err(anyhow!("Missing messages for milestone {}!", ms).into());
                        }
                    };
                    paging_state = messages.paging_state.take();
                    milestone_data.messages_mut().extend(
                        messages
                            .into_iter()
                            .filter(|m| m.message_id != ms_message_id)
                            .map(|m| (m.message_id, m)),
                    );
                    if paging_state.is_none() {
                        break;
                    }
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

async fn query<V, S, K>(
    keyspace: &S,
    key: &K,
    page_size: Option<i32>,
    paging_state: Option<Vec<u8>>,
) -> anyhow::Result<Option<V>>
where
    S: 'static + Select<K, (), V> + Clone,
    K: 'static + Send + Sync + Clone + TokenEncoder,
    V: 'static + Send + Sync + Clone + std::fmt::Debug + RowsDecoder,
{
    let request = keyspace.select::<V>(&key, &()).consistency(Consistency::One);
    Ok(if let Some(page_size) = page_size {
        request.page_size(page_size).paging_state(&paging_state)
    } else {
        request.paging_state(&paging_state)
    }
    .build()?
    .worker()
    .with_retries(3)
    .get_local()
    .await?)
}
