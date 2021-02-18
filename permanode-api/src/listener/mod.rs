use super::*;
use application::*;
use permanode_storage::access::{
    Delete,
    Insert,
    Message,
    MessageId,
    ReporterHandle,
    Select,
    Update,
    Worker,
    WorkerError,
};
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

#[derive(Debug)]
pub enum Event {
    /// Response from scylla with a payload
    Response {
        /// The payload.
        giveload: Vec<u8>,
    },
    /// Error from scylla
    Error {
        /// The Error kind.
        kind: WorkerError,
    },
}

#[derive(Debug)]
pub struct DecoderWorker(pub UnboundedSender<Event>);

impl Worker for DecoderWorker {
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let event = Event::Response { giveload };
        self.0.send(event).expect("AHHHHHH");
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        let event = Event::Error { kind: error };
        self.0.send(event).expect("AHHHHHH");
    }
}

builder!(ListenerBuilder {});

impl Builder for ListenerBuilder {
    type State = Listener;

    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
        }
        .set_name()
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
