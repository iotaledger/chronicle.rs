// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chrono::NaiveDate;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct DailyAnalyticsRecord {
    pub year: u32,
    pub date: NaiveDate,
    pub total_addresses: u32,
    pub send_addresses: u32,
    pub recv_addresses: u32,
}

impl TokenEncoder for DailyAnalyticsRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&self.year).into()
    }
}

impl Row for DailyAnalyticsRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            year: rows.column_value()?,
            date: rows.column_value()?,
            total_addresses: rows.column_value()?,
            send_addresses: rows.column_value()?,
            recv_addresses: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for DailyAnalyticsRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(self.year)
            .value(self.date)
            .value(self.total_addresses)
            .value(self.send_addresses)
            .value(self.recv_addresses)
    }
}
