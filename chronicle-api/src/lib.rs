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
/// The http endpoint listener
pub mod listener;
/// API response structs
pub mod responses;
/// The websocket actor
// pub mod websocket;

#[macro_use]
extern crate rocket;

use async_trait::async_trait;
use backstage::*;
