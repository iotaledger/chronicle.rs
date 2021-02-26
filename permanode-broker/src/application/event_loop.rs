use super::*;

#[async_trait]
impl<H: PermanodeBrokerScope> EventLoop<H> for PermanodeBroker<H> {
    async fn event_loop(
        &mut self,
        _status: Result<(), chronicle::Need>,
        supervisor: &mut Option<H>,
    ) -> Result<(), chronicle::Need> {
        if let Some(ref mut supervisor) = supervisor {
            self.service.update_status(ServiceStatus::Running);
            while let Some(event) = self.inbox.recv().await {
                match event {
                    BrokerEvent::Passthrough(passthrough_events) => match passthrough_events.try_get_my_event() {
                        Ok(my_event) => match my_event {
                            PermanodeBrokerThrough::Shutdown => {
                                if !self.service.is_stopping() {
                                    // Ask launcher to shutdown broker application,
                                    // this is usefull in case the shutdown event sent by the websocket
                                    // client.
                                    supervisor.shutdown_app(&self.get_name());
                                    // shutdown children
                                    if let Some(listener) = self.listener_handle.take() {
                                        listener.shutdown();
                                    }
                                    // drop self handler
                                    self.handle.take();
                                }
                            }
                            PermanodeBrokerThrough::Topology(t) => {
                                todo!("add/remove feed source")
                            }
                        },
                        Err(other_app_event) => {
                            supervisor.passthrough(other_app_event, self.get_name());
                        }
                    },
                    BrokerEvent::Children(child) => {}
                }
            }
            Ok(())
        } else {
            Err(Need::Abort)
        }
    }
}
