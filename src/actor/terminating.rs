use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait Terminating<H: AcknowledgeShutdown<Self>>: Sized {
    async fn terminating(&mut self, status: ResultSource, supervisor: &mut Option<H>) -> NeedResult;
}
