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
