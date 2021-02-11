use std::time::Duration;

use async_trait::async_trait;

pub use builder::*;
pub use end::End;
pub use event_loop::EventLoop;
pub use futures::future::Aborted;
use futures::future::{AbortRegistration, Abortable};
pub use init::Init;
pub use launcher::*;
pub use name::Name;
pub use passthrough::Passthrough;
pub use preparer::Preparer;
pub use start::*;
pub use starter::Starter;
pub use supervisor::*;
pub use terminating::Terminating;
pub use tokio::time::error::Elapsed;

mod builder;
mod end;
mod event_loop;
mod init;
mod launcher;
mod name;
mod passthrough;
mod preparer;
mod start;
mod starter;
mod supervisor;
mod terminating;

#[async_trait]
pub trait Actor<H: AcknowledgeShutdown<Self> + 'static>: Sized {
    fn name(&self) -> String;

    async fn init(&mut self, supervisor: &mut Option<H>) -> NeedResult;

    async fn event_loop(&mut self, supervisor: &mut Option<H>) -> NeedResult;

    async fn terminating(&mut self, status: ResultSource, supervisor: &mut Option<H>) -> NeedResult;

    async fn end(mut self, status: ResultSource, mut supervisor: Option<H>) -> NeedResult {
        let status = self.terminating(status, &mut supervisor).await;
        // aknowledge_shutdown to supervisor if provided
        if let Some(my_supervisor) = supervisor {
            my_supervisor.acknowledge_shutdown(self, status).await;
        }
        Ok(())
    }

    async fn start(mut self, mut supervisor: Option<H>) -> ResultSource {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init(res);
        if res.is_ok() {
            res = self.event_loop(&mut supervisor).await;
            source = ResultSource::Loop(res);
            if res.is_ok() {
                res = self.end(ResultSource::Loop(res), supervisor).await;
                source = ResultSource::End(res);
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
    async fn start_abortable(mut self, abort_registration: AbortRegistration, mut supervisor: Option<H>) -> ResultSource {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init(res);
        if res.is_ok() {
            let abortable_event_loop_fut = Abortable::new(self.event_loop(&mut supervisor), abort_registration);
            match abortable_event_loop_fut.await {
                Ok(mut res) => {
                    if res.is_ok() {
                        res = self.end(ResultSource::Loop(res), supervisor).await;
                        source = ResultSource::End(res);
                    } else {
                        self.end(source, supervisor).await.ok();
                    }
                }
                Err(aborted) => {
                    res = Err(Need::Abort);
                    source = ResultSource::Loop(res);
                    self.aborted(aborted, &mut supervisor);
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
    async fn start_timeout(mut self, duration: Duration, mut supervisor: Option<H>) -> ResultSource {
        let mut res = self.init(&mut supervisor).await;
        let mut source = ResultSource::Init(res);
        if res.is_ok() {
            let timeout_event_loop_fut = tokio::time::timeout(duration, self.event_loop(&mut supervisor));
            match timeout_event_loop_fut.await {
                Ok(mut res) => {
                    if res.is_ok() {
                        res = self.end(ResultSource::Loop(res), supervisor).await;
                        source = ResultSource::End(res);
                    } else {
                        self.end(source, supervisor).await.ok();
                    }
                }
                Err(elapsed) => {
                    res = Err(Need::Abort);
                    source = ResultSource::Loop(res);
                    self.timed_out(elapsed, &mut supervisor);
                    self.end(source, supervisor).await.ok();
                }
            }
        } else {
            self.end(source, supervisor).await.ok();
        }
        source
    }

    fn aborted(&self, aborted: Aborted, supervisor: &mut Option<H>);

    fn timed_out(&self, elapsed: Elapsed, supervisor: &mut Option<H>);
}

#[macro_export]
macro_rules! actor {
    ($t:ty) => {
        #[async_trait]
        impl<H> End<H> for $t
        where
            $t: Terminating<H>,
            H: AcknowledgeShutdown<Self> + 'static,
        {
        }

        #[async_trait]
        impl<H> Actor<H> for $t
        where
            $t: Sized + StartActor<H> + Send,
            H: AcknowledgeShutdown<Self> + 'static,
        {
            fn name(&self) -> String {
                self.name()
            }

            async fn init(&mut self, supervisor: &mut Option<H>) -> NeedResult {
                <Self as Init<H>>::init(self, supervisor).await
            }

            async fn event_loop(&mut self, supervisor: &mut Option<H>) -> NeedResult {
                <Self as EventLoop<H>>::event_loop(self, supervisor).await
            }

            async fn terminating(&mut self, status: ResultSource, supervisor: &mut Option<H>) -> NeedResult {
                <Self as Terminating<H>>::terminating(self, status, supervisor).await
            }

            fn aborted(&self, aborted: Aborted, supervisor: &mut Option<H>) {
                <Self as StartActor<H>>::aborted(self, aborted, supervisor)
            }

            fn timed_out(&self, elapsed: Elapsed, supervisor: &mut Option<H>) {
                <Self as StartActor<H>>::timed_out(self, elapsed, supervisor)
            }
        }
    };
}
