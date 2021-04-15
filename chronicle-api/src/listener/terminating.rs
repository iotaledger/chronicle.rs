use super::*;

#[async_trait]
impl<T: APIEngine, H: ChronicleAPIScope> Terminating<ChronicleAPISender<H>> for Listener<T> {
    async fn terminating(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ChronicleAPISender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(ChronicleAPIEvent::Children(ChronicleAPIChild::Listener(
                    self.service.clone(),
                )))
                .map_err(|_| Need::Abort)
        } else {
            Err(Need::Abort)
        }
    }
}
