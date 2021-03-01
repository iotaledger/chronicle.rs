use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> Terminating<H> for PermanodeBroker<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        status
    }
}
