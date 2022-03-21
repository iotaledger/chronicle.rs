// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rocket::{
    http::Status,
    Rocket,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    borrow::Cow,
    ops::Deref,
};
use thiserror::Error;

#[cfg(feature = "mongo_api")]
mod mongo;
#[cfg(feature = "mongo_api")]
pub use self::mongo::construct_rocket;

//#[cfg(feature = "scylla_api")]
// mod scylla;

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

#[cfg(feature = "mongo_api")]
impl From<mongodb::error::Error> for ListenerError {
    fn from(e: mongodb::error::Error) -> Self {
        Self::Other(e.into())
    }
}

/// A success wrapper for API responses
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SuccessBody<T> {
    data: T,
}

impl<T> Deref for SuccessBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
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
