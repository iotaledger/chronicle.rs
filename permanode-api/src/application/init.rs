use super::*;

#[async_trait]
impl<H: PermanodeAPIScope> Init<H> for PermanodeAPI<H> {
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