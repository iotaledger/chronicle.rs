// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chrono::NaiveDate;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct DateCacheRecord {
    pub date: NaiveDate,
    pub start_ms: MilestoneIndex,
    pub end_ms: MilestoneIndex,
}

impl TokenEncoder for DateCacheRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&self.date).into()
    }
}

impl Row for DateCacheRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            date: rows.column_value()?,
            start_ms: rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
            end_ms: rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
        })
    }
}

impl<B: Binder> Bindable<B> for DateCacheRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(self.date)
            .value(Bee(self.start_ms))
            .value(Bee(self.end_ms))
    }
}
