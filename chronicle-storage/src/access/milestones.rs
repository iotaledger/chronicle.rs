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