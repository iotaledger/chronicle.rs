// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `Basic Output` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct BasicOutputRecord {
    pub output_id: OutputId,
    pub partition_data: PartitionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub address: Address,
    pub sender: Option<Address>,
    pub tag: Option<String>,
    pub data: BasicOutput,
}

impl BasicOutputRecord {
    pub fn new(
        output_id: OutputId,
        partition_data: PartitionData,
        inclusion_state: Option<LedgerInclusionState>,
        data: BasicOutput,
    ) -> Self {
        Self {
            output_id,
            partition_data,
            inclusion_state,
            address: *data.address(),
            sender: data.feature_blocks().sender().map(|fb| *fb.address()),
            tag: data
                .feature_blocks()
                .tag()
                .map(|fb| String::from_utf8_lossy(fb.tag()).into_owned()),
            data,
        }
    }
}
