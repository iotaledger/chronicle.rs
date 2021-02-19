use super::*;

#[async_trait]
impl<H: LauncherSender<BrokerBuilder<H>>> Init<H> for PermanodeBroker<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor.status_change(self.service.clone());
            status
        } else {
            Err(Need::Abort)
        }
    }
}
