// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This module implements the batch query frame.

use super::{
    consistency::Consistency,
    encoder::{ColumnEncoder, BE_0_BYTES_LEN, BE_8_BYTES_LEN, BE_NULL_BYTES_LEN, BE_UNSET_BYTES_LEN},
    header::Header,
    opcode::BATCH,
};
use crate::compression::Compression;

type QueryCount = u16;

/// The batch frame with multiple queries.
pub struct Batch(pub Vec<u8>, pub QueryCount);

#[repr(u8)]
/// The batch type enum.
pub enum BatchTypes {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}

impl Header for Batch {
    fn new() -> Self {
        Batch(Vec::new(), 0)
    }
    fn with_capacity(capacity: usize) -> Self {
        Batch(Vec::with_capacity(capacity), 0)
    }
    fn version(mut self) -> Self {
        self.0.push(4);
        self
    }
    fn flags(mut self, flags: u8) -> Self {
        self.0.push(flags);
        self
    }
    fn stream(mut self, stream: i16) -> Self {
        self.0.extend(&i16::to_be_bytes(stream));
        self
    }
    fn opcode(mut self) -> Self {
        self.0.push(BATCH);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Batch {
    /// Set the batch type in the Batch frame.
    pub fn batch_type(mut self, batch_type: BatchTypes) -> Self {
        // push batch_type and pad zero querycount
        self.0.extend(&[batch_type as u8, 0, 0]);
        self
    }
    /// Set the statement in the Batch frame.
    pub fn statement(mut self, statement: &str) -> Self {
        // normal query
        self.0.push(0);
        self.0.extend(&i32::to_be_bytes(statement.len() as i32));
        self.0.extend(statement.bytes());
        self.1 += 1; // update querycount
        self
    }
    /// Set the id in the Batch frame.
    pub fn id(mut self, id: &str) -> Self {
        // prepared query
        self.0.push(1);
        self.0.extend(&u16::to_be_bytes(id.len() as u16));
        self.0.extend(id.bytes());
        self.1 += 1;
        self
    }
    /// Set the value count in the Batch frame.
    pub fn value_count(mut self, value_count: u16) -> Self {
        self.0.extend(&u16::to_be_bytes(value_count));
        self
    }
    /// Set the value in the Batch frame.
    pub fn value(mut self, value: impl ColumnEncoder) -> Self {
        value.encode(&mut self.0);
        self
    }
    /// Set the value to be unset in the Batch frame.
    pub fn unset_value(mut self) -> Self {
        self.0.extend(&BE_UNSET_BYTES_LEN);
        self
    }
    /// Set the value to be null in the Batch frame.
    pub fn null_value(mut self) -> Self {
        self.0.extend(&BE_NULL_BYTES_LEN);
        self
    }
    /// Set the consistency of the Batch frame.
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    /// Set the batch flags of the Batch frame.
    pub fn batch_flags(mut self, batch_flags: u8) -> Self {
        self.0.push(batch_flags);
        self
    }
    /// Set the serial consistency in the Batch frame.
    pub fn serial_consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    /// Set the timestamp of the Batch frame.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.0.extend(&BE_8_BYTES_LEN);
        self.0.extend(&i64::to_be_bytes(timestamp));
        self
    }
    /// Build a Batch frame.
    pub fn build(mut self, compression: impl Compression) -> Self {
        // adjust the querycount
        self.0[10..12].copy_from_slice(&u16::to_be_bytes(self.1));
        self.0 = compression.compress(self.0);
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compression::UNCOMPRESSED,
        frame::{batchflags::NOFLAGS, header::IGNORE},
        statements::INSERT_TX_QUERY,
    };
    use std::time::{SystemTime, UNIX_EPOCH};
    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let Batch(_payload, _querycount) = Batch::new()
            .version()
            .flags(IGNORE)
            .stream(0)
            .opcode()
            .length()
            .batch_type(BatchTypes::Logged)
            .statement(INSERT_TX_QUERY)
            .value_count(17) // the total value count
            .value("HASH_VALUE")
            .value("PAYLOAD_VALUE")
            .value("ADDRESS_VALUE")
            .value(0 as i64) // tx-value as i64
            .value("OBSOLETE_TAG_VALUE")
            .value(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64) // junk timestamp
            .value(0 as i64) // current-index
            .value(0 as i64) // last-index
            .value("BUNDLE_HASH_VALUE")
            .value("TRUNK_VALUE")
            .value("BRANCH_VALUE")
            .value("TAG_VALUE")
            .value(0 as i64) // attachment_timestamp
            .value(0 as i64) // attachment_timestamp_lower
            .value(0 as i64) // attachment_timestamp_upper
            .value("NONCE_VALUE") // nonce
            .unset_value() // not-set value for milestone
            .id("HASHED_MD5_STATEMENT") // add second query(prepared one) to the batch
            .value_count(1) // value count in the second query
            .value("JUNK_VALUE") // junk value
            .consistency(Consistency::One)
            .batch_flags(NOFLAGS) // no remaing flags
            .build(UNCOMPRESSED); // build uncompressed batch
    }
}
