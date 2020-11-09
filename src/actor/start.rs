use super::*;
use async_trait::async_trait;
use futures::future::{AbortRegistration, Abortable};
use std::time::Duration;

#[async_trait]
pub trait StartActor<H: AknShutdown<Self> + Send + 'static>: Sized + Init<H> + EventLoop<H> + Terminating<H> + End<H> {
    async fn start(mut self, mut supervisor: Option<H>) {
        let mut status = self.init(Ok(()), &mut supervisor).await;
        status = self.event_loop(status, &mut supervisor).await;
        self.end(status, supervisor).await;
    }
    /// This method will start the actor with abortable event loop by using
    /// let (abort_handle, abort_registration) = AbortHandle::new_pair();
    async fn start_abortable(mut self, abort_registration: AbortRegistration, mut supervisor: Option<H>) {
        let status = self.init(Ok(()), &mut supervisor).await;
        let abortable_event_loop_fut = Abortable::new(self.event_loop(status, &mut supervisor), abort_registration);
        if let Ok(status) = abortable_event_loop_fut.await {
            self.end(status, supervisor).await;
        } else {
            self.end(Err(Need::Abort), supervisor).await;
        };
    }
    /// This method will start the actor with timeout/ttl event loop by using
    /// the runtime timer timeout functionality;
    async fn start_timeout(mut self, duration: Duration, mut supervisor: Option<H>) {
        let status = self.init(Ok(()), &mut supervisor).await;
        let timeout_event_loop_fut = tokio::time::timeout(duration, self.event_loop(status, &mut supervisor));
        if let Ok(status) = timeout_event_loop_fut.await {
            self.end(status, supervisor).await;
        } else {
            self.end(Err(Need::Abort), supervisor).await;
        };
    }
}
