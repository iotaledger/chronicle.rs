use super::{
    application::BrokerHandle,
    solidifier::SolidifierHandle,
};
use backstage::core::{
    Actor,
    Channel,
};
use bee_message::{
    Message,
    MessageId,
};
use bee_rest_api::types::responses::MessageMetadataResponse;
use chronicle_storage::access::{
    MessageRecord,
    MilestoneData,
    MilestoneDataBuilder,
    Selected,
};

use scylla_rs::prelude::*;
use serde::Serialize;
use std::fmt::Debug;
#[async_trait::async_trait]

pub trait FilterBuilder:
    'static + Debug + PartialEq + Eq + Sized + Send + Clone + Serialize + Sync + std::default::Default
{
    type Actor: Actor<BrokerHandle>;
    async fn build(&self) -> anyhow::Result<(Self::Actor, <Self::Actor as Actor<BrokerHandle>>::Channel)>;
    async fn filter_message(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        message: &MessageRecord,
    ) -> anyhow::Result<Option<Selected>>;
    async fn process_milestone_data_builder(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData>;
}

/// Atomic process handle
#[derive(Debug)]
pub struct AtomicProcessHandle {
    pub(crate) handle: tokio::sync::oneshot::Sender<Result<u32, u32>>,
    pub(crate) milestone_index: u32,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}

impl AtomicProcessHandle {
    /// Create a new Atomic solidifier handle
    pub fn new(handle: tokio::sync::oneshot::Sender<Result<u32, u32>>, milestone_index: u32) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            handle,
            milestone_index,
            any_error: std::sync::atomic::AtomicBool::new(false),
        })
    }
    /// set any_error to true
    pub fn set_error(&self) {
        self.any_error.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for AtomicProcessHandle {
    fn drop(&mut self) {
        let any_error = self.any_error.load(std::sync::atomic::Ordering::Acquire);
        if any_error {
            // respond with err
            todo!()
            // self.handle.send(Ok(self.milestone_index)).ok();
        } else {
            // respond with void
            todo!()
            // self.handle.send(Err(self.milestone_index)).ok();
        }
    }
}

/// Scylla worker implementation
#[derive(Clone, Debug)]
pub struct AtomicProcessWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Debug + Sync,
    V: 'static + Send + Debug + Sync,
{
    handle: std::sync::Arc<AtomicProcessHandle>,
    keyspace: S,
    key: K,
    value: V,
    retries: u8,
}

impl<S: Insert<K, V>, K, V> AtomicProcessWorker<S, K, V>
where
    S: 'static + Insert<K, V>,
    K: 'static + Send + Debug + Sync,
    V: 'static + Send + Debug + Sync,
{
    /// Create a new atomic solidifier worker with a handle and retries
    pub fn new(handle: std::sync::Arc<AtomicProcessHandle>, keyspace: S, key: K, value: V, retries: u8) -> Self {
        Self {
            handle,
            keyspace,
            key,
            value,
            retries,
        }
    }
    /// Create a new boxed atomic solidifier worker with a handle and retries
    pub fn boxed(handle: std::sync::Arc<AtomicProcessHandle>, keyspace: S, key: K, value: V, retries: u8) -> Box<Self> {
        Box::new(Self::new(handle, keyspace, key, value, retries))
    }
}

impl<S, K, V> Worker for AtomicProcessWorker<S, K, V>
where
    S: 'static + Insert<K, V> + Debug,
    K: 'static + Send + Clone + Debug + Sync + TokenEncoder,
    V: 'static + Send + Clone + Debug + Sync,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        Decoder::try_from(giveload).and_then(|decoder| decoder.get_void())
    }
    fn handle_error(
        mut self: Box<Self>,
        mut error: WorkerError,
        reporter: Option<&ReporterHandle>,
    ) -> anyhow::Result<()> {
        if let WorkerError::Cql(ref mut cql_error) = error {
            if let (Some(id), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.statement();
                PrepareWorker::new(id, statement.into()).send_to_reporter(reporter).ok();
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            // currently we assume all cql/worker errors are retryable, but we might change this in future
            match self
                .keyspace
                .insert_query(&self.key, &self.value)
                .consistency(Consistency::Quorum)
                .build()
            {
                Ok(req) => {
                    let keyspace_name = self.keyspace.name();
                    if let Err(RequestError::Ring(r)) = req.send_global_with_worker(self) {
                        if let Err(worker) = retry_send(&keyspace_name, r, 2) {
                            worker.handle_error(WorkerError::NoRing, None)?
                        };
                    };
                }
                Err(e) => {
                    log::error!("{}", e);
                    self.handle.set_error();
                }
            }
        } else {
            // no more retries
            self.handle.set_error();
        }
        Ok(())
    }
}
