// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `Legacy Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct LegacyOutputRecord {
    pub output_id: OutputId,
    pub output_type: OutputType,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub address: Address,
    pub data: Output,
}

impl LegacyOutputRecord {
    pub fn new(
        output_id: OutputId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: Output,
    ) -> anyhow::Result<Self> {
        ensure!(
            matches!(data, Output::SignatureLockedSingle(_)) || matches!(data, Output::SignatureLockedDustAllowance(_)),
            "Invalid output type"
        );
        Ok(Self {
            output_id,
            output_type: data.kind(),
            partition_data,
            inclusion_state,
            address: match &data {
                Output::SignatureLockedSingle(output) => *output.address(),
                Output::SignatureLockedDustAllowance(output) => *output.address(),
                _ => unreachable!(),
            },
            data,
        })
    }
}

impl Partitioned for LegacyOutputRecord {
    const MS_CHUNK_SIZE: u32 = BasicOutputRecord::MS_CHUNK_SIZE;
}

impl Row for LegacyOutputRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::new(
            rows.column_value::<Bee<OutputId>>()?.into_inner(),
            PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            rows.column_value()?,
            rows.column_value::<Bee<Output>>()?.into_inner(),
        )?)
    }
}

impl TokenEncoder for LegacyOutputRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.output_id)).into()
    }
}

impl<B: Binder> Bindable<B> for LegacyOutputRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.output_id))
            .value(self.output_type)
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(Bee(self.address))
            .value(Bee(&self.data))
    }
}
