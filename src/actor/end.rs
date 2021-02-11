use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait End<H: AcknowledgeShutdown<Self> + Send + 'static>: Terminating<H> {
    async fn end(mut self, status: ResultSource, mut supervisor: Option<H>) -> NeedResult {
        let status = self.terminating(status, &mut supervisor).await;
        // aknowledge_shutdown to supervisor if provided
        if let Some(my_supervisor) = supervisor {
            my_supervisor.acknowledge_shutdown(self, status).await;
        }
        Ok(())
    }
}
