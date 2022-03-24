// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod cpt2;
mod shimmer;

use std::str::FromStr;

use super::{
    Message,
    MessageId,
    MessageRecord,
};
use mongodb::bson::{
    doc,
    from_document,
    to_bson,
    Bson,
    Document,
};

impl Into<Bson> for &Message {
    fn into(self) -> Bson {
        match self {
            Message::Chrysalis(m) => cpt2::message_to_bson(m),
            Message::Shimmer(m) => shimmer::message_to_bson(m),
        }
    }
}

impl Into<Document> for MessageRecord {
    fn into(self) -> Document {
        doc! {
            "message_id": self.message_id.to_string(),
            "message": Into::<Bson>::into(&self.message),
            "milestone_index": self.milestone_index,
            "inclusion_state": self.inclusion_state.map(|i| i as u8 as i32),
            "conflict_reason": self.conflict_reason.map(|i| i as u8 as i32),
            "proof": to_bson(&self.proof).unwrap(),
            "protocol_version": self.protocol_version as i32,
        }
    }
}

impl TryFrom<&Document> for Message {
    type Error = anyhow::Error;

    fn try_from(value: &Document) -> Result<Self, Self::Error> {
        let protocol_version = value.get_i32("protocol_version")?;
        Ok(match protocol_version {
            0 => Message::Chrysalis(cpt2::message_from_doc(value)?),
            _ => Message::Shimmer(shimmer::message_from_doc(value)?),
        })
    }
}

impl TryFrom<&Document> for MessageRecord {
    type Error = anyhow::Error;

    fn try_from(value: &Document) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: MessageId::from_str(value.get_str("message_id")?)?,
            message: value.get_document("message")?.try_into()?,
            milestone_index: value.get_i32("milestone_index").ok().map(|i| i as u32),
            inclusion_state: value
                .get_i32("inclusion_state")
                .ok()
                .map(|i| (i as u8).try_into())
                .transpose()?,
            conflict_reason: value
                .get_i32("conflict_reason")
                .ok()
                .map(|i| (i as u8).try_into())
                .transpose()?,
            proof: value
                .get_document("proof")
                .ok()
                .cloned()
                .map(|p| from_document(p))
                .transpose()?,
            protocol_version: value.get_i32("protocol_version")? as u8,
        })
    }
}
