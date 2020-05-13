use super::{
    consistency::Consistency,
    encoder::{
        ColumnEncoder,
        BE_0_BYTES_LEN,
        BE_8_BYTES_LEN,
        BE_NULL_BYTES_LEN,
        BE_UNSET_BYTES_LEN,
    },
    header::{
        self,
        Header,
    },
    opcode::EXECUTE,
    queryflags::{
        PAGE_SIZE,
        PAGING_STATE,
        SERIAL_CONSISTENCY,
        SKIP_METADATA,
        TIMESTAMP,
        VALUES,
    },
};
use crate::compression::{
    Compression,
    UNCOMPRESSED,
};
pub struct Execute(Vec<u8>);

impl Header for Execute {
    fn new() -> Self {
        Execute(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Execute(Vec::with_capacity(capacity))
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
    fn opcode(mut self, opcode: u8) -> Self {
        self.0.push(opcode);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Execute {
    fn id(mut self, id: &str) -> Self {
        self.0.extend(&u16::to_be_bytes(id.len() as u16));
        self.0.extend(id.bytes());
        self
    }
    fn consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    fn query_flags(mut self, query_flags: u8) -> Self {
        self.0.push(query_flags);
        self
    }
    fn value(mut self, value: impl ColumnEncoder) -> Self {
        value.encode(&mut self.0);
        self
    }
    fn unset_value(mut self) -> Self {
        self.0.extend(&BE_UNSET_BYTES_LEN);
        self
    }
    fn null_value(mut self) -> Self {
        self.0.extend(&BE_NULL_BYTES_LEN);
        self
    }
    fn page_size(mut self, page_size: i32) -> Self {
        self.0.extend(&i32::to_be_bytes(page_size));
        self
    }
    fn paging_state(mut self, paging_state: String) -> Self {
        self.0.extend(&i32::to_be_bytes(paging_state.len() as i32));
        self.0.extend(paging_state.bytes());
        self
    }
    fn serial_consistency(mut self, consistency: Consistency) -> Self {
        self.0.extend(&u16::to_be_bytes(consistency as u16));
        self
    }
    fn timestamp(mut self, timestamp: i64) -> Self {
        self.0.extend(&BE_8_BYTES_LEN);
        self.0.extend(&i64::to_be_bytes(timestamp));
        self
    }
    fn build(mut self, compression: impl Compression) -> Self {
        compression.compress(&mut self.0);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    };
    #[test]
    // note: junk data
    fn simple_execute_builder_test() {
        let Execute(payload) = Execute::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode(EXECUTE)
            .length()
            .id("TestID")
            .consistency(Consistency::One)
            .query_flags(SKIP_METADATA | VALUES)
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
            .build(UNCOMPRESSED); // build uncompressed
    }
}
