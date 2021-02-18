use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> Init<PermanodeSender<H>> for Listener {
    async fn init(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(PermanodeEvent::Children(PermanodeChild::Listener(self.service.clone())))
                .map_err(|_| Need::Abort)
        } else {
            Err(Need::Abort)
        }
    }
}
