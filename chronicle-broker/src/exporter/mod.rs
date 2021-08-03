// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::archiver::{
    Archiver,
    ArchiverEvent,
};
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
    responder: tokio::sync::mpsc::UnboundedSender<ExporterStatus>,
}

#[derive(Serialize, Deserialize)]
pub enum ExporterStatus {
    InProgress { current: u32, completed: u32, total: u32 },
    Done,
    Failed(String),
}

#[build]
pub fn build_exporter(
    ms_range: Range<u32>,
    keyspace: ChronicleKeyspace,
    responder: tokio::sync::mpsc::UnboundedSender<ExporterStatus>,
) -> Exporter {
    Exporter {
        ms_range,
        keyspace,
        responder,
    }
}

#[async_trait]
impl Actor for Exporter {
    type Dependencies = Act<Archiver>;
    type Event = ();
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.actor_event_handle::<Archiver>()
            .await
            .ok_or_else(|| anyhow::anyhow!("No archiver is running!"))?;
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        archiver: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        for ms in self.ms_range.clone() {
            debug!("Processing milestone {}", ms);
            self.responder
                .send(ExporterStatus::InProgress {
                    current: ms,
                    completed: ms - self.ms_range.start,
                    total: self.ms_range.end - self.ms_range.start,
                })
                .ok();
            let mut milestone_data = MilestoneData::new(ms, CreatedBy::Exporter);
            if let Some(milestone) =
                query::<Milestone, _, _>(&self.keyspace, &MilestoneIndex::from(ms), None, None).await?
            {
                debug!("Found milestone data {}", milestone.message_id());
                let milestone = query::<FullMessage, _, _>(&self.keyspace, milestone.message_id(), None, None)
                    .await?
                    .ok_or_else(|| {
                        self.responder
                            .send(ExporterStatus::Failed(format!("Missing milestone message {}!", ms)))
                            .ok();
                        anyhow::anyhow!("Missing milestone message {}!", ms)
                    })?;
                debug!("Found milestone message {}", milestone.message_id());
                let mut parent_queue = milestone.message().parents().iter().cloned().collect::<VecDeque<_>>();
                if let Some(Payload::Milestone(ms_payload)) = milestone.message().payload() {
                    milestone_data.set_milestone(ms_payload.clone());
                    milestone_data.add_full_message(milestone);
                } else {
                    self.responder
                        .send(ExporterStatus::Failed(format!("Missing milestone payload for {}!", ms)))
                        .ok();
                    return Err(anyhow::anyhow!("Missing milestone payload for {}!", ms).into());
                }
                let mut visited = HashSet::new();
                while let Some(parent) = parent_queue.pop_front() {
                    debug!("Processing message {}", parent);
                    visited.insert(parent);
                    let message = query::<FullMessage, _, _>(&self.keyspace, &parent, None, None).await?;
                    if let Some(message) = message {
                        debug!("Found message {}", message.metadata().message_id);
                        if let Some(ref_ms) = message.metadata().referenced_by_milestone_index {
                            if ms == ref_ms {
                                parent_queue
                                    .extend(message.message().parents().iter().filter(|p| !visited.contains(*p)));
                                milestone_data.add_full_message(message);
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
                            .send(ExporterStatus::Failed(format!("Missing message {}!", parent)))
                            .ok();
                        return Err(anyhow::anyhow!("Missing message {}!", parent).into());
                    }
                }
                debug!("Finished processing milestone {}", ms);
                debug!("Milestone Data:\n{:?}", milestone_data);
                archiver.send(ArchiverEvent::MilestoneData(milestone_data, None))?;
            }
        }
        archiver.send(ArchiverEvent::Close(self.ms_range.end)).ok();
        self.responder.send(ExporterStatus::Done).ok();
        rt.update_status(ServiceStatus::Stopped).await.ok();
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
    S: 'static + Select<K, V> + Clone,
    K: 'static + Send + Sync + Clone,
    V: 'static + Send + Sync + Clone,
{
    let request = keyspace.select::<V>(key).consistency(Consistency::One);
    let request = if let Some(page_size) = page_size {
        request.page_size(page_size).paging_state(&paging_state)
    } else {
        request.paging_state(&paging_state)
    }
    .build()?;
    let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel::<Result<Option<V>, WorkerError>>();
    let mut worker = ValueWorker::new(sender, keyspace.clone(), key.clone(), 0, PhantomData);
    if let Some(page_size) = page_size {
        worker = worker.with_paging(page_size, paging_state);
    }
    let worker = Box::new(worker);

    request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        return event.map_err(|e| anyhow::anyhow!(e));
    }

    anyhow::bail!("No response!")
}
