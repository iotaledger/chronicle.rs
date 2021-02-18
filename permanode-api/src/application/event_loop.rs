use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<H> for Permanode<H> {
    async fn event_loop(
        &mut self,
        status: Result<(), chronicle::Need>,
        supervisor: &mut Option<H>,
    ) -> Result<(), chronicle::Need> {
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    PermanodeEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            PermanodeThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    // Ask launcher to shutdown scylla application,
                                    // this is usefull in case the shutdown event sent by the websocket
                                    // client.
                                    supervisor.shutdown_app(&self.get_name());
                                    // shutdown children
                                    self.listener.abort();
                                    // make sure to drop self handler
                                    self.sender.take();
                                }
                            }
                            PermanodeThrough::AddNode(_) => {}
                            PermanodeThrough::RemoveNode(_) => {}
                            PermanodeThrough::TryBuild(_) => {}
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    PermanodeEvent::Children(child) => match child {
                        PermanodeChild::Listener(service) => {
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
