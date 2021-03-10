#![warn(missing_docs)]

//! # Permanode API
//!
//! This crate defines HTTP endpoints and topics to be used by a
//! dashboard to explore Permanode stored tangle data.
//!
//! ### HTTP Endpoints
//! - `/<network>`
//!     - `/messages`
//!         - `?<index>`
//!         - `/<message_id>`
//!         - `/<message_id>/metadata`
//!         - `/<message_id>/children`
//!     - `/outputs/<output_id>`
//!     - `/addresses/ed25519/<address>/outputs`
//!     - `/milestones/<index>`

/// The main actor for the API
pub mod application;
/// API configuration
pub mod config;
/// The http endpoint listener
pub mod listener;
/// The websocket actor
pub mod websocket;

#[macro_use]
extern crate rocket;

use async_trait::async_trait;
use chronicle::*;
pub use config::ApiConfig;
