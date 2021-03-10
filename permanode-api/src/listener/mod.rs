use super::*;
use application::*;
use permanode_storage::access::{
    worker::PrepareWorker,
    GetSelectRequest,
    GetSelectStatement,
    Message,
    ReporterEvent,
    ReporterHandle,
    Request,
    Select,
    Worker,
    WorkerError,
};
use rocket::Rocket;
use scylla_cql::{
    Consistency,
    Prepare,
};
use serde::Serialize;
use std::marker::PhantomData;
use tokio::sync::mpsc::UnboundedSender;

mod init;
mod rocket_event_loop;
mod terminating;
mod warp_event_loop;

/// A listener implementation using Rocket.rs
pub struct RocketListener {
    rocket: Option<Rocket>,
}

impl RocketListener {
    /// Create a rocket listener data structure using a Rocket instance
    pub fn new(rocket: Rocket) -> Self {
        Self { rocket: Some(rocket) }
    }
}

/// A listener implementation using Warp
pub struct WarpListener;

/// A listener. Can use Rocket or Warp depending on data provided
pub struct Listener<T> {
    /// The listener's service
    pub service: Service,
    data: T,
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
        stringify!(WarpListener)
    }
}

/// A listener event
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

/// A worker used to decode responses from the Ring with result sets
#[derive(Debug)]
pub struct DecoderWorker<S: Select<K, V>, K, V> {
    pub sender: UnboundedSender<Event>,
    pub keyspace: S,
    pub key: K,
    pub value: PhantomData<V>,
}

impl<S, K, V> Worker for DecoderWorker<S, K, V>
where
    S: 'static + Select<K, V> + std::fmt::Debug,
    K: 'static + Send + std::fmt::Debug + Clone,
    V: 'static + Send + std::fmt::Debug + Clone,
{
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
        let event = Event::Response { giveload };
        self.sender.send(event).expect("AHHHHHH");
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &Option<ReporterHandle>) {
        if let WorkerError::Cql(mut cql_error) = error {
            if let (Some(_), Some(reporter)) = (cql_error.take_unprepared_id(), reporter) {
                let statement = self.keyspace.select_statement::<K, V>();
                let prepare = Prepare::new().statement(&statement).build();
                let prepare_worker = PrepareWorker {
                    retries: 3,
                    payload: prepare.0.clone(),
                };
                let prepare_request = ReporterEvent::Request {
                    worker: Box::new(prepare_worker),
                    payload: prepare.0,
                };
                reporter.send(prepare_request).ok();
                let req = self
                    .keyspace
                    .select::<V>(&self.key)
                    .consistency(Consistency::One)
                    .build();
                let payload = req.payload().clone();
                let retry_request = ReporterEvent::Request { worker: self, payload };
                reporter.send(retry_request).ok();
            }
        } else {
            let event = Event::Error { kind: error };
            self.sender.send(event).expect("AHHHHHH");
        }
    }
}

builder!(ListenerBuilder<T> {
    data: T
});

impl<T: APIEngine> Builder for ListenerBuilder<T> {
    type State = Listener<T>;

    fn build(self) -> Self::State {
        Self::State {
            service: Service::new(),
            data: self.data.expect("No listener data was provided!"),
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

/// A success wrapper for API responses
#[derive(Clone, Debug, Serialize)]
pub struct SuccessBody<T> {
    data: T,
}

impl<T> SuccessBody<T> {
    /// Create a new SuccessBody from any inner type
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> From<T> for SuccessBody<T> {
    fn from(data: T) -> Self {
        Self::new(data)
    }
}
