// Copyright 2022 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
//! The analytics errors.

use thiserror::Error;

/// The Analytics Error code.
#[derive(Error, Debug)]
pub enum AnalyticsError {
    /// No results are returned.
    #[error("No results returned!")]
    NoResults,
    /// Other errors.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
