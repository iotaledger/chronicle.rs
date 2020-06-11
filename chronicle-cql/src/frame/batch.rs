use super::{
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_0_BYTES_LEN,
        BE_8_BYTES_LEN,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    header::Header,
    opcode::BATCH,
};
use crate::compression::Compression;

type QueryCount = u16;

pub struct Batch(pub Vec<u8>, pub QueryCount);

#[repr(u8)]
pub enum BatchTypes {
    Logged = 0,
    Unlogged = 1,
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
    pub fn batch_type(mut self, batch_type: BatchTypes) -> Self {
        // push batch_type and pad zero querycount
        self.0.extend(&[batch_type as u8, 0, 0]);
        self
    }
    pub fn statement(mut self, statement: &str) -> Self {
        // normal query
        self.0.push(0);
        self.0.extend(&i32::to_be_bytes(statement.len() as i32));
        self.0.extend(statement.bytes());
        self.1 += 1; // update querycount
        self
    }
    pub fn id(mut self, id: &str) -> Self {
        // prepared query
        self.0.push(1);
        self.0.extend(&u16::to_be_bytes(id.len() as u16));
        self.0.extend(id.bytes());
        self.1 += 1;
        self
    }
    pub fn value_count(mut self, value_count: u16) -> Self {
        self.0.extend(&u16::to_be_bytes(value_count));
        self
    }
    pub fn value(mut self, value: impl ColumnEncoder) -> Self {
        value.encode(&mut self.0);
        self
    }
    pub fn unset_value(mut self) -> Self {
        self.0.extend(&BE_UNSET_BYTES_LEN);
        self
    }
    pub fn null_value(mut self) -> Self {
        self.0.extend(&BE_NULL_BYTES_LEN);
        self
    }
    pub fn consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    pub fn batch_flags(mut self, batch_flags: u8) -> Self {
        self.0.push(batch_flags);
        self
    }
    pub fn serial_consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.0.extend(&BE_8_BYTES_LEN);
        self.0.extend(&i64::to_be_bytes(timestamp));
        self
    }
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
        frame::{
            batchflags::NOFLAGS,
            header::IGNORE,
        },
        statements::INSERT_TX_QUERY,
    };
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
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
