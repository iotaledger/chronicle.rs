use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait End<H: AknShutdown<Self> + Send + 'static>: Terminating<H> {
    async fn end(mut self, mut status: Result<(), Need>, mut supervisor: Option<H>) {
        status = self.terminating(status, &mut supervisor).await;
        // aknowledge_shutdown to supervisor if provided
        if let Some(my_supervisor) = supervisor {
            my_supervisor.aknowledge_shutdown(self, status).await;
        }
    }
}
