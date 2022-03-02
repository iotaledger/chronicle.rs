// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Debug)]
pub struct MilestoneRecord {
    milestone_index: MilestoneIndex,
    message_id: MessageId,
    timestamp: NaiveDateTime,
    payload: MilestonePayload,
}

impl MilestoneRecord {
    pub fn new(milestone_index: MilestoneIndex, message_id: MessageId, payload: MilestonePayload) -> Self {
        Self {
            milestone_index,
            message_id,
            timestamp: NaiveDateTime::from_timestamp(payload.essence().timestamp() as i64, 0),
            payload,
        }
    }
}

impl TokenEncoder for MilestoneRecord {
    fn encode_token(&self) -> TokenEncodeChain {
        (&Bee(&self.milestone_index)).into()
    }
}

impl Row for MilestoneRecord {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            milestone_index: rows.column_value::<Bee<MilestoneIndex>>()?.into_inner(),
            message_id: rows.column_value::<Bee<MessageId>>()?.into_inner(),
            timestamp: rows.column_value()?,
            payload: rows.column_value::<Bee<MilestonePayload>>()?.into_inner(),
        })
    }
}

impl<B: Binder> Bindable<B> for MilestoneRecord {
    fn bind(&self, binder: B) -> B {
        binder
            .value(Bee(self.milestone_index))
            .value(Bee(self.message_id))
            .value(self.timestamp)
            .value(Bee(&self.payload))
    }
}
