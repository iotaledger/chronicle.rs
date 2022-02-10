// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `Foundry Output` table record
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct FoundryOutputRecord {
    pub foundry_id: FoundryId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub address: Address,
    pub data: FoundryOutput,
}

impl FoundryOutputRecord {
    pub fn new(
        foundry_id: FoundryId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: FoundryOutput,
    ) -> Self {
        Self {
            foundry_id,
            partition_data,
            inclusion_state,
            address: (*data.address()).into(),
            data,
        }
    }
}

impl Partitioned for FoundryOutputRecord {
    const MS_CHUNK_SIZE: u32 = BasicOutputRecord::MS_CHUNK_SIZE;
}

impl Row for FoundryOutputRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            foundry_id: rows.column_value::<Bee<FoundryId>>()?.into_inner(),
            partition_data: PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            inclusion_state: rows.column_value()?,
            address: rows.column_value::<Bee<Address>>()?.into_inner(),
            data: rows.column_value::<Bee<FoundryOutput>>()?.into_inner(),
        })
    }
}

impl TokenEncoder for FoundryOutputRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.foundry_id)).into()
    }
}

impl<B: Binder> Bindable<B> for FoundryOutputRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.foundry_id))
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(Bee(self.address))
            .value(Bee(&self.data))
    }
}
