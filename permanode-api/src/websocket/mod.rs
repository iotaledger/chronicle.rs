use super::*;
use application::*;

pub mod commands;
pub mod event;
mod event_loop;
mod init;
mod terminating;
pub mod topics;

pub struct Websocket {
    pub service: Service,
}

builder!(WebsocketBuilder {});

impl Builder for WebsocketBuilder {
    type State = Websocket;

    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
        }
        .set_name()
    }
}

impl Name for Websocket {
    fn set_name(mut self) -> Self {
        self.service.update_name("Websocket".into());
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<H: PermanodeAPIScope> AknShutdown<Websocket> for PermanodeAPISender<H> {
    async fn aknowledge_shutdown(self, mut state: Websocket, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
