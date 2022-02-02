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
use bee_message::{
    milestone::Milestone,
    payload::Payload,
    prelude::MilestoneIndex,
};
use std::{
    collections::{
        HashSet,
        VecDeque,
    },
    ops::Range,
};

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
            let mut milestone_data = MilestoneData::new(ms, CreatedBy::Exporter);
            if let Some(milestone) =
                query::<Bee<Milestone>, _, _>(&self.keyspace, &Bee(MilestoneIndex::from(ms)), None, None).await?
            {
                let ms_message_id = milestone.message_id();
                debug!("Found milestone data {}", milestone.message_id());
                let milestone =
                    match query::<FullMessage, _, _>(&self.keyspace, &Bee(*ms_message_id), None, None).await? {
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
                let mut parent_queue = milestone.message().parents().iter().cloned().collect::<VecDeque<_>>();
                if let Some(Payload::Milestone(ms_payload)) = milestone.message().payload() {
                    milestone_data.set_milestone(*ms_message_id, ms_payload.clone());
                    milestone_data.add_full_message(milestone, None);
                } else {
                    self.responder
                        .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                            "Missing milestone payload for {}!",
                            ms
                        )))))
                        .await
                        .ok();
                    return Err(anyhow!("Missing milestone payload for {}!", ms).into());
                }
                let mut visited = HashSet::new();
                while let Some(parent) = parent_queue.pop_front() {
                    debug!("Processing message {}", parent);
                    visited.insert(parent);
                    let message = query::<FullMessage, _, _>(&self.keyspace, &Bee(parent), None, None).await?;
                    if let Some(message) = message {
                        debug!("Found message {}", message.metadata().message_id);
                        if let Some(ref_ms) = message.metadata().referenced_by_milestone_index {
                            if ms == ref_ms {
                                parent_queue
                                    .extend(message.message().parents().iter().filter(|p| !visited.contains(*p)));
                                milestone_data.add_full_message(message, None);
                            } else {
                                // warn!(
                                //    "Message {} is referenced by other milestone {}",
                                //    message.metadata().message_id,
                                //    ref_ms
                                //);
                                continue;
                            }
                        } else {
                            error!("Unreferenced message {}", message.metadata().message_id);
                            // TODO: handle messages without milestone reference
                            continue;
                        }
                    } else {
                        error!("Missing message {}", parent);
                        // TODO: maybe request missing messages? since we can't verify whether a message belongs to
                        // this milestone unless we have it in the database, we might need to
                        self.responder
                            .reply(Ok(TopologyOk::Export(ExporterStatus::Failed(format!(
                                "Missing message {}!",
                                parent
                            )))))
                            .await
                            .ok();
                        return Err(anyhow!("Missing message {}!", parent).into());
                    }
                }
                debug!("Finished processing milestone {}", ms);
                debug!("Milestone Data:\n{:?}", milestone_data);
                archiver
                    .send(ArchiverEvent::MilestoneData(
                        std::sync::Arc::new(milestone_data),
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
