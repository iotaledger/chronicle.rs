// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
pub use prometheus;
use prometheus::{
    HistogramOpts,
    HistogramVec,
    IntCounter,
    IntCounterVec,
    Opts,
    Registry,
};

lazy_static! {
    /// Metrics registry
    pub static ref REGISTRY: Registry = Registry::new();
    /// Incoming request counter
    pub static ref INCOMING_REQUESTS: IntCounter =
        IntCounter::new("incoming_requests", "Incoming Requests").expect("failed to create metric");
    /// Response code collector
    pub static ref RESPONSE_CODE_COLLECTOR: IntCounterVec = IntCounterVec::new(
        Opts::new("response_code", "Response Codes"),
        &["statuscode", "type"]
    )
    .expect("failed to create metric");
    /// Response time collector
    pub static ref RESPONSE_TIME_COLLECTOR: HistogramVec =
        HistogramVec::new(HistogramOpts::new("response_time", "Response Times"), &["endpoint"])
            .expect("failed to create metric");
}
