use super::*;

#[async_trait]
impl<H: ChronicleAPIScope> Init<ChronicleAPISender<H>> for Websocket {
    async fn init(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<ChronicleAPISender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
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
