use super::*;
use async_trait::async_trait;
use futures::future::{AbortRegistration, Abortable, Aborted};
use std::time::Duration;
use tokio::time::error::Elapsed;

#[derive(Copy, Clone)]
pub enum ResultSource {
    Init(NeedResult),
    Loop(NeedResult),
    End(NeedResult),
}

#[async_trait]
pub trait StartActor<H: AcknowledgeShutdown<Self> + Send + 'static>: Sized + Init<H> + EventLoop<H> + End<H> {
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
