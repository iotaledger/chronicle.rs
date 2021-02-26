use super::*;
use application::*;
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod event_loop;
mod init;
mod terminating;

pub struct AddFeedSource {
    pub service: Service,
    pub inbox: UnboundedReceiver<AddFeedSourceEvent>,
}

builder!(AddFeedSourceBuilder {});

impl Builder for AddFeedSourceBuilder {
    type State = AddFeedSource;

    fn build(self) -> Self::State {
        todo!()
    }
}

impl Name for AddFeedSource {
    fn set_name(mut self) -> Self {
        self.service.update_name("Add Feed Source".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

pub enum AddFeedSourceEvent {}

#[async_trait::async_trait]
impl<H: PermanodeAPIScope> AknShutdown<AddFeedSource> for PermanodeAPISender<H> {
    async fn aknowledge_shutdown(self, mut state: AddFeedSource, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
