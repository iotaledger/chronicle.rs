use std::{
    future::Future,
    pin::Pin,
    task::{
        Context,
        Poll,
    },
};

use super::*;
use application::*;
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

mod event_loop;
mod init;
mod terminating;

pub struct Listener {
    pub service: Service,
}

builder!(ListenerBuilder {});

impl Builder for ListenerBuilder {
    type State = Listener;

    fn build(self) -> Self::State {
        todo!()
    }
}

impl Name for Listener {
    fn set_name(mut self) -> Self {
        self.service.update_name("Listener".to_string());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> AknShutdown<Listener> for PermanodeSender<H> {
    async fn aknowledge_shutdown(self, mut state: Listener, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
