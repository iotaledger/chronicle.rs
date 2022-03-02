// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// A `parents` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct ParentRecord {
    pub parent_id: MessageId,
    pub message_id: MessageId,
    pub milestone_index: Option<MilestoneIndex>,
    pub ms_timestamp: Option<NaiveDateTime>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    /// Creates a new parent row
    pub fn new(
        parent_id: MessageId,
        milestone_index: Option<MilestoneIndex>,
        ms_timestamp: Option<NaiveDateTime>,
        message_id: MessageId,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            parent_id,
            milestone_index,
            ms_timestamp,
            message_id,
            inclusion_state,
        }
    }
}

impl TokenEncoder for ParentRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.parent_id)).into()
    }
}

impl Row for ParentRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            parent_id: rows.column_value::<Bee<MessageId>>()?.into_inner(),
            message_id: rows.column_value::<Bee<MessageId>>()?.into_inner(),
            milestone_index: rows
                .column_value::<Option<Bee<MilestoneIndex>>>()?
                .map(|a| a.into_inner()),
            ms_timestamp: rows.column_value()?,
            inclusion_state: rows.column_value()?,
        })
    }
}

impl<B: Binder> Bindable<B> for ParentRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.parent_id))
            .value(Bee(self.message_id))
            .value(self.milestone_index.as_ref().map(Bee))
            .value(self.ms_timestamp)
            .value(self.inclusion_state.as_ref().map(|l| *l as u8))
    }
}
