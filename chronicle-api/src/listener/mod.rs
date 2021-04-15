use super::*;
use application::*;
use chronicle_common::get_config_async;
use chronicle_storage::access::*;
use rocket::{
    http::Status,
    Rocket,
};
use serde::Serialize;
use std::{
    borrow::Cow,
    marker::PhantomData,
};
use thiserror::Error;

mod init;
#[cfg(feature = "rocket_listener")]
mod rocket_event_loop;
mod terminating;

#[derive(Error, Debug)]
enum ListenerError {
    #[error("No results returned!")]
    NoResults,
    #[error("No response from scylla!")]
    NoResponseError,
    #[error("Provided index is too large! (Max 64 bytes)")]
    IndexTooLarge,
    #[error("Invalid hexidecimal encoding!")]
    InvalidHex,
    #[error("Specified keyspace ({0}) is not configured!")]
    InvalidKeyspace(String),
    #[error("Invalid state provided!")]
    InvalidState,
    #[error("No endpoint found!")]
    NotFound,
    #[error(transparent)]
    BadParse(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ListenerError {
    pub fn status(&self) -> Status {
        match self {
            ListenerError::NoResults | ListenerError::InvalidKeyspace(_) => Status::NotFound,
            ListenerError::IndexTooLarge | ListenerError::InvalidHex | ListenerError::BadParse(_) => Status::BadRequest,
            _ => Status::InternalServerError,
        }
    }

    pub fn code(&self) -> u16 {
        self.status().code
    }
}

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

/// A listener. Can use Rocket or another impl depending on data provided
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
impl<T: APIEngine, H: ChronicleAPIScope> AknShutdown<Listener<T>> for ChronicleAPISender<H> {
    async fn aknowledge_shutdown(self, mut state: Listener<T>, _status: Result<(), Need>) {
        state.service.update_status(ServiceStatus::Stopped);
    }
}

/// A success wrapper for API responses
#[derive(Clone, Debug, Serialize)]
struct SuccessBody<T> {
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

#[derive(Clone, Debug, Serialize)]
struct ErrorBody {
    #[serde(skip_serializing)]
    status: Status,
    code: u16,
    message: Cow<'static, str>,
}

impl From<ListenerError> for ErrorBody {
    fn from(err: ListenerError) -> Self {
        Self {
            status: err.status(),
            code: err.code(),
            message: err.to_string().into(),
        }
    }
}
