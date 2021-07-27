// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]

//! # Chronicle API
//!
//! This crate defines HTTP endpoints and topics to be used by a
//! dashboard to explore Chronicle stored tangle data.
//!
//! ### HTTP Endpoints
//! - `/api/<keyspace>`
//!     - `/messages`
//!         - `?<index>[&<page_size>]`
//!         - `/<message_id>`
//!         - `/<message_id>/metadata`
//!         - `/<message_id>/children[?<page_size>]`
//!     - `/outputs/<output_id>`
//!     - `/addresses/ed25519/<address>/outputs[?<page_size>]`
//!     - `/milestones/<index>`

/// The main actor for the API
pub mod application;

#[cfg(feature = "rocket_listener")]
/// The http endpoint listener
pub mod listener;
/// API response structs
pub mod responses;
/// The websocket actor
pub mod websocket;

#[cfg(feature = "rocket_listener")]
#[macro_use]
extern crate rocket;

use async_trait::async_trait;
use backstage::prelude::*;
use scylla_rs::prelude::*;
pub use websocket::ChronicleRequest;
