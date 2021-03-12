use scylla::{
    access::*,
    application::{
        error,
        info,
    },
};
use std::marker::PhantomData;
mod insert;
mod prepare;
mod select;

pub use insert::InsertWorker;
pub use prepare::PrepareWorker;
pub use select::SelectWorker;

/// A usual worker event type
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
