// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! The worker trait to process the ScyllaDB requests.

pub mod preparer;
pub mod schema_cql;
use crate::stage::reporter::{Giveload, Sender};
use chronicle_cql::frame::error::CqlError;
use std::{error::Error as StdError, fmt};

/// WorkerId trait type which will be implemented by worker in order to send their channel_tx.
pub trait Worker: Send + std::fmt::Debug {
    /// Send the response.
    fn send_response(self: Box<Self>, tx: &Option<Sender>, giveload: Giveload);
    /// Send the error.
    fn send_error(self: Box<Self>, error: Error);
}
#[derive(Debug)]
/// The CQL worker error.
pub enum Error {
    /// The CQL Error reported from ScyllaDB.
    Cql(CqlError),
    /// The IO Error.
    Io(std::io::Error),
    /// The overload when we do not have any more streams.
    Overload,
    /// We lost the worker due to the abortion of ScyllaDB connection.
    Lost,
    /// There is no ring initialized.
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
