use std::marker::PhantomData;

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

mod init;
mod rocket_event_loop;
mod terminating;
mod warp_event_loop;

pub struct RocketListener;
pub struct WarpListener;

pub struct Listener<T> {
    pub service: Service,
    _data: PhantomData<T>,
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

builder!(ListenerBuilder<T> {});

impl<T> Builder for ListenerBuilder<T> {
    type State = Listener<T>;

    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
            _data: PhantomData,
        }
        .set_name()
    }
}

impl<T> Name for Listener<T> {
    fn set_name(mut self) -> Self {
        self.service.update_name(format!("{} Listener", stringify!(T)));
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<T: 'static + Send, H: LauncherSender<PermanodeBuilder<H>>> AknShutdown<Listener<T>> for PermanodeSender<H> {
    async fn aknowledge_shutdown(self, mut state: Listener<T>, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
