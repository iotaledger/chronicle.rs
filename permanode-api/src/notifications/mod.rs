use super::*;
use application::*;
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod event_loop;
mod init;
mod terminating;

pub struct Notifications {
    pub service: Service,
    pub inbox: UnboundedReceiver<AddFeedSourceEvent>,
}

builder!(NotificationsBuilder {});

impl Builder for NotificationsBuilder {
    type State = Notifications;

    fn build(self) -> Self::State {
        todo!()
    }
}

impl Name for Notifications {
    fn set_name(mut self) -> Self {
        self.service.update_name("Notifications".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

pub enum AddFeedSourceEvent {}

#[async_trait::async_trait]
impl<H: PermanodeAPIScope> AknShutdown<Notifications> for PermanodeAPISender<H> {
    async fn aknowledge_shutdown(self, mut state: Notifications, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
