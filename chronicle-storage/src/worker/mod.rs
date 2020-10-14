pub mod preparer;
pub mod schema_cql;
use crate::stage::reporter::{Giveload, Sender};
use chronicle_cql::frame::error::CqlError;
use std::{error::Error as StdError, fmt};

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
        match self {
            Error::Cql(cql_error) => write!(f, "Worker CqlError: {:?}", cql_error),
            Error::Io(io_error) => write!(f, "Worker IoError: {:?}", io_error),
            Error::Overload => write!(f, "Worker Overload"),
            Error::Lost => write!(f, "Worker Lost"),
            Error::NoRing => write!(f, "Worker NoRing"),
        }
    }
}

impl StdError for Error {}
