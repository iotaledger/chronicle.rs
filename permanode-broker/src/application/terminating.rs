use super::*;

#[async_trait]
impl<H: LauncherSender<BrokerBuilder<H>>> Terminating<H> for PermanodeBroker<H> {
    async fn terminating(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        status
    }
}
