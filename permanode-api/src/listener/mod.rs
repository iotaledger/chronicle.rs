use super::*;
use application::*;
use permanode_storage::access::*;
use rocket::Rocket;
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
