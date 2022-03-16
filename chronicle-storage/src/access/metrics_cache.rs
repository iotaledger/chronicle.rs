// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use chrono::NaiveDate;
use std::fmt::Display;

use super::*;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct MetricsCacheRecord {
    pub date: NaiveDate,
    pub variant: MetricsVariant,
    pub value: String,
    pub metric_value: Vec<u8>,
}

pub struct MetricsCacheCount(pub u64);
impl MetricsCacheCount {
    pub fn new(count: u64) -> Self {
        Self(count)
    }
}
impl From<u64> for MetricsCacheCount {
    fn from(count: u64) -> Self {
        Self(count)
    }
}
impl Deref for MetricsCacheCount {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for MetricsCacheCount {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Copy, Clone, Debug)]
pub enum MetricsVariant {
    Address(AddressMetrics),
    Index(IndexMetrics),
}

#[derive(Copy, Clone, Debug)]
pub enum AddressMetrics {
    Sent,
    Received,
    SentOrReceived,
}

impl Display for AddressMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sent => write!(f, "sent"),
            Self::Received => write!(f, "received"),
            Self::SentOrReceived => write!(f, "sent_or_received"),
        }
    }
}

impl FromStr for AddressMetrics {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "sent" => Ok(Self::Sent),
            "received" => Ok(Self::Received),
            "sent_or_received" => Ok(Self::SentOrReceived),
            _ => Err(anyhow!("Invalid address metrics variant: {}", s)),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum IndexMetrics {
    Used,
}

impl Display for IndexMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Used => write!(f, "used"),
        }
    }
}

impl FromStr for IndexMetrics {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "used" => Ok(IndexMetrics::Used),
            _ => Err(anyhow::anyhow!("Invalid index metrics variant: {}", s)),
        }
    }
}
