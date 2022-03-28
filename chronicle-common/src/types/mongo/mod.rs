// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod cpt2;
mod shimmer;

use super::{
    Message,
    MessageId,
    MessageRecord,
};
use anyhow::anyhow;
use mongodb::bson::{
    doc,
    document::ValueAccessError,
    from_bson,
    to_bson,
    Bson,
    Document,
};
use std::str::FromStr;

impl Into<Bson> for &Message {
    fn into(self) -> Bson {
        match self {
            Message::Chrysalis(m) => cpt2::message_to_bson(m),
            Message::Shimmer(m) => shimmer::message_to_bson(m),
        }
    }
}

impl Into<Document> for &MessageRecord {
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

impl TryFrom<Bson> for Message {
    type Error = anyhow::Error;

    fn try_from(value: Bson) -> Result<Self, Self::Error> {
        value.to_document()?.try_into()
    }
}

impl TryFrom<Document> for Message {
    type Error = anyhow::Error;

    fn try_from(value: Document) -> Result<Self, Self::Error> {
        let protocol_version = value.get_i32("protocol_version")?;
        Ok(match protocol_version {
            0 => Message::Chrysalis(cpt2::message_from_doc(value)?),
            _ => Message::Shimmer(shimmer::message_from_doc(value)?),
        })
    }
}

impl TryFrom<Document> for MessageRecord {
    type Error = anyhow::Error;

    fn try_from(mut value: Document) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: MessageId::from_str(value.get_str("message_id")?)?,
            message: value.take("message")?.try_into()?,
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
            proof: value.take("proof").ok().map(|p| from_bson(p)).transpose()?,
            protocol_version: value.get_i32("protocol_version")? as u8,
        })
    }
}

/// Gets values and upcasts if necessary
pub trait BsonExt {
    fn as_string(&self) -> Result<String, ValueAccessError>;

    fn as_u8(&self) -> Result<u8, ValueAccessError>;

    fn as_u16(&self) -> Result<u16, ValueAccessError>;

    fn as_u32(&self) -> Result<u32, ValueAccessError>;

    fn as_u64(&self) -> Result<u64, ValueAccessError>;

    fn to_array(self) -> Result<Vec<Bson>, ValueAccessError>;

    fn to_document(self) -> Result<Document, ValueAccessError>;
}

impl BsonExt for Bson {
    fn as_string(&self) -> Result<String, ValueAccessError> {
        Ok(match self {
            Bson::Double(i) => i.to_string(),
            Bson::String(i) => i.to_string(),
            Bson::Document(i) => i.to_string(),
            Bson::Boolean(i) => i.to_string(),
            Bson::Null => "null".to_string(),
            Bson::RegularExpression(i) => i.to_string(),
            Bson::JavaScriptCode(i) => i.to_string(),
            Bson::JavaScriptCodeWithScope(i) => i.to_string(),
            Bson::Int32(i) => i.to_string(),
            Bson::Int64(i) => i.to_string(),
            Bson::Timestamp(i) => i.to_string(),
            Bson::Binary(i) => i.to_string(),
            Bson::ObjectId(i) => i.to_string(),
            Bson::DateTime(i) => i.to_string(),
            Bson::Symbol(i) => i.to_string(),
            Bson::Decimal128(i) => i.to_string(),
            Bson::Undefined => "undefined".to_string(),
            _ => return Err(ValueAccessError::UnexpectedType),
        })
    }

    fn as_u8(&self) -> Result<u8, ValueAccessError> {
        Ok(match self {
            Bson::Double(i) => *i as u8,
            Bson::Boolean(i) => *i as u8,
            Bson::Int32(i) => *i as u8,
            Bson::Int64(i) => *i as u8,
            _ => return Err(ValueAccessError::UnexpectedType),
        })
    }

    fn as_u16(&self) -> Result<u16, ValueAccessError> {
        Ok(match self {
            Bson::Double(i) => *i as u16,
            Bson::Boolean(i) => *i as u16,
            Bson::Int32(i) => *i as u16,
            Bson::Int64(i) => *i as u16,
            _ => return Err(ValueAccessError::UnexpectedType),
        })
    }

    fn as_u32(&self) -> Result<u32, ValueAccessError> {
        Ok(match self {
            Bson::Double(i) => *i as u32,
            Bson::Boolean(i) => *i as u32,
            Bson::Int32(i) => *i as u32,
            Bson::Int64(i) => *i as u32,
            Bson::Timestamp(i) => i.time,
            _ => return Err(ValueAccessError::UnexpectedType),
        })
    }

    fn as_u64(&self) -> Result<u64, ValueAccessError> {
        Ok(match self {
            Bson::Double(i) => *i as u64,
            Bson::Boolean(i) => *i as u64,
            Bson::Int32(i) => *i as u64,
            Bson::Int64(i) => *i as u64,
            Bson::Timestamp(i) => i.time as u64,
            _ => return Err(ValueAccessError::UnexpectedType),
        })
    }

    fn to_array(self) -> Result<Vec<Bson>, ValueAccessError> {
        match self {
            Bson::Array(i) => Ok(i),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    fn to_document(self) -> Result<Document, ValueAccessError> {
        match self {
            Bson::Document(i) => Ok(i),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }
}

pub trait DocExt {
    fn take(&mut self, key: impl AsRef<str>) -> anyhow::Result<Bson>;

    fn take_array(&mut self, key: impl AsRef<str>) -> anyhow::Result<Vec<Bson>>;

    fn take_document(&mut self, key: impl AsRef<str>) -> anyhow::Result<Document>;

    fn get_as_string(&self, key: impl AsRef<str>) -> anyhow::Result<String>;

    fn get_as_u8(&self, key: impl AsRef<str>) -> anyhow::Result<u8>;

    fn get_as_u16(&self, key: impl AsRef<str>) -> anyhow::Result<u16>;

    fn get_as_u32(&self, key: impl AsRef<str>) -> anyhow::Result<u32>;

    fn get_as_u64(&self, key: impl AsRef<str>) -> anyhow::Result<u64>;
}

impl DocExt for Document {
    fn take(&mut self, key: impl AsRef<str>) -> anyhow::Result<Bson> {
        let bson = self
            .remove(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?;
        match bson {
            Bson::Null => Err(anyhow!("Value for key {} is null", key.as_ref())),
            _ => Ok(bson),
        }
    }

    fn take_array(&mut self, key: impl AsRef<str>) -> anyhow::Result<Vec<Bson>> {
        Ok(self.take(key)?.to_array()?)
    }

    fn take_document(&mut self, key: impl AsRef<str>) -> anyhow::Result<Document> {
        Ok(self.take(key)?.to_document()?)
    }

    fn get_as_string(&self, key: impl AsRef<str>) -> anyhow::Result<String> {
        Ok(self
            .get(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?
            .as_string()?)
    }

    fn get_as_u8(&self, key: impl AsRef<str>) -> anyhow::Result<u8> {
        Ok(self
            .get(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?
            .as_u8()?)
    }

    fn get_as_u16(&self, key: impl AsRef<str>) -> anyhow::Result<u16> {
        Ok(self
            .get(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?
            .as_u16()?)
    }

    fn get_as_u32(&self, key: impl AsRef<str>) -> anyhow::Result<u32> {
        Ok(self
            .get(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?
            .as_u32()?)
    }

    fn get_as_u64(&self, key: impl AsRef<str>) -> anyhow::Result<u64> {
        Ok(self
            .get(key.as_ref())
            .ok_or_else(|| anyhow!("Missing key {}", key.as_ref()))?
            .as_u64()?)
    }
}
