// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// An `Alias Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct AliasOutputRecord {
    pub alias_id: AliasId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub sender: Option<Address>,
    pub issuer: Option<Address>,
    pub state_controller: Address,
    pub governor: Address,
    pub data: AliasOutput,
}

impl AliasOutputRecord {
    pub fn new(
        alias_id: AliasId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: AliasOutput,
    ) -> Self {
        Self {
            alias_id,
            partition_data,
            inclusion_state,
            sender: data.feature_blocks().sender().map(|fb| *fb.address()),
            issuer: data.feature_blocks().issuer().map(|fb| *fb.address()),
            state_controller: *data.state_controller_address(),
            governor: *data.governor_address(),
            data,
        }
    }
}
