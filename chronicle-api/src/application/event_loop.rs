use super::*;

#[async_trait]
impl<H: ChronicleAPIScope> EventLoop<H> for ChronicleAPI<H> {
    async fn event_loop(&mut self, _status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    ChronicleAPIEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            ChronicleAPIThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    supervisor.shutdown_app(&self.get_name());
                                    // shutdown children
                                    self.rocket_listener.take().map(|handle| handle.shutdown());
                                    // self.websocket.abort();
                                    // make sure to drop self handler
                                    self.sender.take();
                                }
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    ChronicleAPIEvent::Children(child) => match child {
                        ChronicleAPIChild::Listener(service) | ChronicleAPIChild::Websocket(service) => {
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
