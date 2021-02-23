use super::*;

#[async_trait]
impl<T: 'static + Send, H: LauncherSender<PermanodeBuilder<H>>> Terminating<PermanodeSender<H>> for Listener<T> {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(PermanodeEvent::Children(PermanodeChild::Listener(self.service.clone())))
                .map_err(|_| Need::Abort)
        } else {
            Err(Need::Abort)
        }
    }
}
