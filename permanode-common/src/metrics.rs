use chronicle::{
    Service,
    ServiceStatus,
};
use lazy_static::lazy_static;
use log::error;
pub use prometheus;
use prometheus::{
    Encoder,
    HistogramOpts,
    HistogramVec,
    IntCounter,
    IntCounterVec,
    IntGauge,
    IntGaugeVec,
    Opts,
    Registry,
    TextEncoder,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        RwLock,
    },
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
