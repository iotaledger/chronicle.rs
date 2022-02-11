// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A milestone-based analytics record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MsAnalyticsRecord {
    #[serde(skip_serializing)]
    pub ms_range_id: u32,
    pub milestone_index: MilestoneIndex,
    pub message_count: u32,
    pub transaction_count: u32,
    pub transferred_tokens: u64,
}

impl Partitioned for MsAnalyticsRecord {
    const MS_CHUNK_SIZE: u32 = 250_000;
}

impl TokenEncoder for MsAnalyticsRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&self.ms_range_id).into()
    }
}

impl Row for MsAnalyticsRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            ms_range_id: rows.column_value()?,
            milestone_index: rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
            message_count: rows.column_value()?,
            transaction_count: rows.column_value()?,
            transferred_tokens: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for MsAnalyticsRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(self.ms_range_id)
            .value(Bee(self.milestone_index))
            .value(self.message_count)
            .value(self.transaction_count)
            .value(self.transferred_tokens)
    }
}
