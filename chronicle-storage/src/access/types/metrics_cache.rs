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
impl ColumnDecoder for MetricsCacheCount {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(MetricsCacheCount(u64::try_decode_column(slice)?))
    }
}

impl TokenEncoder for MetricsCacheRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&self.date).into()
    }
}

impl Row for MetricsCacheRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            date: rows.column_value()?,
            variant: {
                let variant = rows.column_value::<String>()?;
                match variant.as_str() {
                    "address" => MetricsVariant::Address(AddressMetrics::from_str(&rows.column_value::<String>()?)?),
                    "index" => MetricsVariant::Index(IndexMetrics::from_str(&rows.column_value::<String>()?)?),
                    _ => Err(anyhow!("Invalid address metrics variant: {}", variant))?,
                }
            },
            value: rows.column_value()?,
            metric_value: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for MetricsCacheRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(self.date)
            .bind(self.variant)
            .value(&self.value)
            .value(&self.metric_value)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum MetricsVariant {
    Address(AddressMetrics),
    Index(IndexMetrics),
}

impl<B: Binder> Bindable<B> for MetricsVariant {
    fn bind(&self, binder: B) -> B {
        match self {
            Self::Address(address) => binder.value("address").value(address.to_string()),
            Self::Index(index) => binder.value("index").value(index.to_string()),
        }
    }
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
