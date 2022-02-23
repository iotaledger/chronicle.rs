// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A milestone-based analytics record
#[derive(Clone, Debug)]
pub struct AddressAnalyticsRecord {
    pub address: Address,
    pub milestone_index: MilestoneIndex,
    pub sent_tokens: u64,
    pub recv_tokens: u64,
}

impl TokenEncoder for AddressAnalyticsRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.address)).into()
    }
}

impl Row for AddressAnalyticsRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            address: rows.column_value::<Bee<Address>>()?.into_inner(),
            milestone_index: rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
            sent_tokens: rows.column_value()?,
            recv_tokens: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for AddressAnalyticsRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.address))
            .value(Bee(self.milestone_index))
            .value(self.sent_tokens)
            .value(self.recv_tokens)
    }
}
