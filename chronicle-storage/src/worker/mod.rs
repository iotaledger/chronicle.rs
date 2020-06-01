pub mod preparer;
pub mod schema_cql;
use std::error::Error as StdError;
use std::fmt;
use crate::stage::reporter::{
    Giveload,
    Sender,
};
use chronicle_cql::frame::error::CqlError;

// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + std::fmt::Debug {
    fn send_response(self: Box<Self>, tx: &Option<Sender>, giveload: Giveload);
    fn send_error(self: Box<Self>, error: Error);
}

#[derive(Debug)]
pub enum Error {
    Cql(CqlError),
    Io(std::io::Error),
    Overload,
    Lost,
    NoRing,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Worker Error!")
    }
}

impl StdError for Error {}
