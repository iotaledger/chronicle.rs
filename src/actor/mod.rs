use std::time::Duration;

use async_trait::async_trait;

pub use builder::*;
pub use futures::future::Aborted;
use futures::future::{AbortRegistration, Abortable};
pub use launcher::*;
pub use passthrough::Passthrough;
pub use preparer::Preparer;
pub use starter::Starter;
pub use supervisor::*;
pub use tokio::time::error::Elapsed;

mod builder;
mod launcher;
mod passthrough;
mod preparer;
mod starter;
mod supervisor;

#[derive(Copy, Clone)]
pub enum ResultSource {
    Init,
    Loop,
    End,
}

pub type NeedResult = Result<(), Need>;

#[derive(Copy, Clone)]
pub struct ActorResult {
    pub result: NeedResult,
    pub source: ResultSource,
}

impl ActorResult {
    pub fn new(result: NeedResult, source: ResultSource) -> Self {
        ActorResult { result, source }
    }
}

impl ResultSource {
    pub fn res(self, result: NeedResult) -> ActorResult {
        ActorResult { result, source: self }
    }
}

#[async_trait]
pub trait Actor<H: AcknowledgeShutdown<Self> + 'static>: Sized {
    fn name(&self) -> String;

    async fn init(&mut self, supervisor: &mut Option<H>) -> NeedResult;

    async fn event_loop(&mut self, supervisor: &mut Option<H>) -> NeedResult;

    async fn terminating(&mut self, status: ActorResult, supervisor: &mut Option<H>) -> NeedResult;

    async fn end(mut self, status: ActorResult, mut supervisor: Option<H>) -> NeedResult {
        let status = ResultSource::End.res(self.terminating(status, &mut supervisor).await);
        // aknowledge_shutdown to supervisor if provided
        if let Some(my_supervisor) = supervisor {
            my_supervisor.acknowledge_shutdown(self, status).await;
        }
        Ok(())
    }

    async fn start(mut self, mut supervisor: Option<H>) -> ActorResult {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init.res(res);
        if res.is_ok() {
            res = self.event_loop(&mut supervisor).await;
            source = ResultSource::Loop.res(res);
            if res.is_ok() {
                res = self.end(ResultSource::Loop.res(res), supervisor).await;
                source = ResultSource::End.res(res);
            } else {
                self.end(source, supervisor).await.ok();
            }
        } else {
            self.end(source, supervisor).await.ok();
        }
        source
    }
    /// This method will start the actor with abortable event loop by using
    /// let (abort_handle, abort_registration) = AbortHandle::new_pair();
    async fn start_abortable(mut self, abort_registration: AbortRegistration, mut supervisor: Option<H>) -> ActorResult {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init.res(res);
        if res.is_ok() {
            let abortable_event_loop_fut = Abortable::new(self.event_loop(&mut supervisor), abort_registration);
            match abortable_event_loop_fut.await {
                Ok(mut res) => {
                    if res.is_ok() {
                        res = self.end(ResultSource::Loop.res(res), supervisor).await;
                        source = ResultSource::End.res(res);
                    } else {
                        self.end(source, supervisor).await.ok();
                    }
                }
                Err(aborted) => {
                    res = Err(Need::Abort(AbortType::Manual));
                    source = ResultSource::Loop.res(res);
                    self.end(source, supervisor).await.ok();
                }
            }
        } else {
            self.end(source, supervisor).await.ok();
        }
        source
    }
    /// This method will start the actor with timeout/ttl event loop by using
    /// the runtime timer timeout functionality;
    async fn start_timeout(mut self, duration: Duration, mut supervisor: Option<H>) -> ActorResult {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init.res(res);
        if res.is_ok() {
            let timeout_event_loop_fut = tokio::time::timeout(duration, self.event_loop(&mut supervisor));
            match timeout_event_loop_fut.await {
                Ok(mut res) => {
                    if res.is_ok() {
                        res = self.end(ResultSource::Loop.res(res), supervisor).await;
                        source = ResultSource::End.res(res);
                    } else {
                        self.end(source, supervisor).await.ok();
                    }
                }
                Err(elapsed) => {
                    res = Err(Need::Abort(AbortType::Timeout));
                    source = ResultSource::Loop.res(res);
                    self.end(source, supervisor).await.ok();
                }
            }
        } else {
            self.end(source, supervisor).await.ok();
        }
        source
    }
}
