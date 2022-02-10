// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `tag` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct TagRecord {
    pub tag: String,
    pub partition_data: PartitionData,
    pub message_id: MessageId,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl TagRecord {
    /// Creates a new tag record
    pub fn new(
        tag: String,
        partition_data: PartitionData,
        message_id: MessageId,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            tag,
            partition_data,
            message_id,
            inclusion_state,
        }
    }
}

impl Partitioned for TagRecord {
    const MS_CHUNK_SIZE: u32 = 100_000;
}

impl TokenEncoder for TagRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&self.tag).into()
    }
}

impl<B: Binder> Bindable<B> for TagRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(&self.tag)
            .bind(self.partition_data)
            .value(Bee(self.message_id))
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
    }
}
