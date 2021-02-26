use super::*;
use application::*;
use permanode_storage::{
    access::{
        Message,
        ReporterHandle,
        Select,
        Worker,
        WorkerError,
    },
    config::Config,
};
use std::marker::PhantomData;
use tokio::sync::mpsc::UnboundedSender;

mod init;
mod rocket_event_loop;
mod terminating;
mod warp_event_loop;

pub struct RocketListener;
pub struct WarpListener;

pub struct Listener<T> {
    pub service: Service,
    pub config: Config,
    _data: PhantomData<T>,
}

/// Trait to be implemented on the API engines (ie Rocket, warp, etc)
pub trait APIEngine: Send + 'static {
    /// API Engine name
    fn name() -> &'static str;
}

impl APIEngine for RocketListener {
    fn name() -> &'static str {
        stringify!(RocketListener)
    }
}

impl APIEngine for WarpListener {
    fn name() -> &'static str {
        stringify!(RocketListener)
    }
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

builder!(ListenerBuilder<T> {
    config: Config
});

impl<T: APIEngine> Builder for ListenerBuilder<T> {
    type State = Listener<T>;

    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
            config: self.config.expect("No config was provided!"),
            _data: PhantomData,
        }
        .set_name()
    }
}

impl<T: APIEngine> Name for Listener<T> {
    fn set_name(mut self) -> Self {
        self.service.update_name(format!("{} Listener", T::name()));
        self
    }

    fn get_name(&self) -> String {
        self.service.get_name()
    }
}

#[async_trait::async_trait]
impl<T: APIEngine, H: PermanodeAPIScope> AknShutdown<Listener<T>> for PermanodeAPISender<H> {
    async fn aknowledge_shutdown(self, mut state: Listener<T>, status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}
