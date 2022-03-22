// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
pub use alert::*;

/// Alert config mod
pub mod alert;
#[cfg(feature = "mongodb")]
/// Mongo DB config
pub mod mongo;
