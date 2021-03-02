use super::*;

#[async_trait]
impl<H: PermanodeAPIScope> EventLoop<H> for PermanodeAPI<H> {
    async fn event_loop(
        &mut self,
        _status: Result<(), chronicle::Need>,
        supervisor: &mut Option<H>,
    ) -> Result<(), chronicle::Need> {
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    PermanodeAPIEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            PermanodeAPIThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    supervisor.shutdown_app(&self.get_name());
                                    // shutdown children
                                    self.listener.take().map(|handle| handle.shutdown());
                                    self.websocket.abort();
                                    // make sure to drop self handler
                                    self.sender.take();
                                }
                            }
                            PermanodeAPIThrough::AddNode(_) => {}
                            PermanodeAPIThrough::RemoveNode(_) => {}
                            PermanodeAPIThrough::TryBuild(_) => {}
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    PermanodeAPIEvent::Children(child) => match child {
                        PermanodeAPIChild::Listener(service) | PermanodeAPIChild::Websocket(service) => {
                            self.service.update_microservice(service.get_name(), service);
                            supervisor.status_change(self.service.clone());
                        }
                    },
                }
            }
            Ok(())
        } else {
            Err(Need::Abort)
        }
    }
}
