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
