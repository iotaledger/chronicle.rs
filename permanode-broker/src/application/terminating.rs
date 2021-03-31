use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> Terminating<H> for PermanodeBroker<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        // Invoke shutdown in case we got aborted
        if let Err(Need::Abort) = status {
            self.shutdown(_supervisor.as_mut().unwrap()).await;
        }
        status
    }
}
