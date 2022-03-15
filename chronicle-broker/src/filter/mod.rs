use super::{
    application::BrokerHandle,
    solidifier::SolidifierHandle,
};
use backstage::core::{
    Actor,
    Channel,
};
use bee_message::MessageId;
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
    async fn process_milestone_data(
        &self,
        handle: &<<Self::Actor as Actor<BrokerHandle>>::Channel as Channel>::Handle,
        milestone_data: MilestoneDataBuilder,
    ) -> anyhow::Result<MilestoneData>;
    // todo helper methods
}

/// Atomic process handle
#[derive(Debug)]
pub struct AtomicProcessHandle {
    pub(crate) handle: Option<tokio::sync::oneshot::Sender<Result<u32, u32>>>,
    pub(crate) milestone_index: u32,
    pub(crate) any_error: std::sync::atomic::AtomicBool,
}

impl AtomicProcessHandle {
    /// Create a new Atomic solidifier handle
    pub fn new(handle: tokio::sync::oneshot::Sender<Result<u32, u32>>, milestone_index: u32) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            handle: Some(handle),
            milestone_index,
            any_error: std::sync::atomic::AtomicBool::new(false),
        })
    }
    /// return any error atomic bool
    pub fn any_error(&self) -> &std::sync::atomic::AtomicBool {
        &self.any_error
    }
    /// Create atomic worker
    pub fn atomic_worker<R: Into<CommonRequest> + Request>(
        self: std::sync::Arc<Self>,
        request: R,
        retries: u8,
    ) -> AtomicProcessWorker {
        AtomicProcessWorker::new(self, request, retries)
    }
}

impl Drop for AtomicProcessHandle {
    fn drop(&mut self) {
        let any_error = self.any_error.load(std::sync::atomic::Ordering::Acquire);
        if any_error {
            // respond with err
            self.handle.take().and_then(|h| h.send(Ok(self.milestone_index)).ok());
        } else {
            // respond with void
            self.handle.take().and_then(|h| h.send(Err(self.milestone_index)).ok());
        }
    }
}

pub trait Inherent: Clone {
    fn atomic_worker<R: Into<CommonRequest> + Request>(self, request: R, retries: u8) -> Box<dyn Worker + 'static>;
    fn set_error(&self) {}
}
impl Inherent for std::sync::Arc<AtomicProcessHandle> {
    fn atomic_worker<R: Request + Into<CommonRequest>>(self, request: R, retries: u8) -> Box<dyn Worker + 'static> {
        Box::new(self.atomic_worker(request, retries))
    }
    fn set_error(&self) {
        self.any_error().store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Basic handle, used to insert new message, where consistency is not important
#[derive(Clone, Debug)]
pub struct BasicHandle;

impl Inherent for BasicHandle {
    fn atomic_worker<R: Request + Into<CommonRequest>>(self, request: R, retries: u8) -> Box<dyn Worker + 'static> {
        BasicWorker::new()
    }
}
/// Scylla worker implementation
#[derive(Clone, Debug)]
pub struct AtomicProcessWorker {
    pub request: CommonRequest,
    pub retries: u8,
    handle: std::sync::Arc<AtomicProcessHandle>,
}

impl AtomicProcessWorker {
    /// Create a new atomic worker with a handle and retries
    pub fn new<R: Into<CommonRequest>>(handle: std::sync::Arc<AtomicProcessHandle>, request: R, retries: u8) -> Self {
        Self {
            handle,
            request: request.into(),
            retries,
        }
    }
    /// Create a new boxed atomic worker with a handle and retries
    pub fn boxed<R: Into<CommonRequest>>(
        handle: std::sync::Arc<AtomicProcessHandle>,
        request: R,
        retries: u8,
    ) -> Box<Self> {
        Box::new(Self::new::<R>(handle, request, retries))
    }
}

impl Worker for AtomicProcessWorker {
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
                // check if reporter handle not closed
                if !reporter.is_closed() {
                    let statement = self.request.statement();
                    PrepareWorker::new(id, statement.try_into().unwrap())
                        .send_to_reporter(reporter)
                        .ok();
                    let mut query = Query::from_payload_unchecked(self.request.payload());
                    if let Ok(_) = query.convert_to_query(&self.request.statement().to_string()) {
                        let retry_request = ReporterEvent::Request {
                            worker: self,
                            payload: query.0,
                        };
                        reporter.send(retry_request).ok();
                        return Ok(());
                    }
                }
            }
        }
        if self.retries > 0 {
            self.retries -= 1;
            let payload = self.request.payload();
            let token = self.request.token();
            let keyspace = self.request.keyspace();
            if let Err(r) = send_global(keyspace.as_ref().map(|ks| ks.as_str()), token, payload, self) {
                if let Err(worker) = retry_send(keyspace.as_ref().map(|ks| ks.as_str()), r, 2) {
                    worker.handle_error(WorkerError::NoRing, None)?
                }; // it went through
            }
        } else {
            // no more retries
            self.handle.set_error();
        }
        Ok(())
    }
}
