// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `Legacy Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct LegacyOutputRecord {
    pub output_id: OutputId,
    pub output_type: OutputType,
    pub is_used: OutputSpendKind,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub message_id: MessageId,
    pub amount: Option<u64>,
    pub address: Address,
    pub data: Option<Output>,
}

impl LegacyOutputRecord {
    pub fn created(
        output_id: OutputId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: Output,
        message_id: MessageId,
    ) -> anyhow::Result<Self> {
        ensure!(
            matches!(data, Output::SignatureLockedSingle(_)) || matches!(data, Output::SignatureLockedDustAllowance(_)),
            "Invalid output type"
        );
        Ok(Self {
            output_id,
            output_type: data.kind(),
            is_used: OutputSpendKind::Created,
            partition_data,
            inclusion_state,
            message_id,
            amount: Some(data.amount()),
            address: match &data {
                Output::SignatureLockedSingle(output) => *output.address(),
                Output::SignatureLockedDustAllowance(output) => *output.address(),
                _ => unreachable!(),
            },
            data: Some(data),
        })
    }

    pub fn used(
        output_id: OutputId,
        output_type: OutputType,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        message_id: MessageId,
        amount: Option<u64>,
        address: Address,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            output_id,
            output_type,
            is_used: OutputSpendKind::Used,
            partition_data,
            inclusion_state,
            message_id,
            amount,
            address,
            data: None,
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
        Ok(Self {
            output_id: rows.column_value::<Bee<OutputId>>()?.into_inner(),
            output_type: rows.column_value()?,
            is_used: rows.column_value()?,
            partition_data: PartitionData::new(
                rows.column_value()?,
                rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
                rows.column_value()?,
            ),
            inclusion_state: rows.column_value()?,
            message_id: rows.column_value::<Bee<MessageId>>()?.into_inner(),
            amount: rows.column_value()?,
            address: rows.column_value::<Bee<Address>>()?.into_inner(),
            data: rows.column_value::<Option<Bee<Output>>>()?.map(|i| i.into_inner()),
        })
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
            .value(self.is_used)
            .bind(self.partition_data)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
            .value(Bee(self.message_id))
            .value(self.amount)
            .value(Bee(self.address))
            .value(self.data.as_ref().map(Bee))
    }
}
