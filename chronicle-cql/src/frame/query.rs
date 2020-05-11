use super::consistency::Consistency;
use super::header::Header;
use super::encoder::ColumnEncoder;
// query flags
pub const VALUES: u8 = 0x01;
pub const SKIP_METADATA: u8 = 0x02;
pub const PAGE_SIZE: u8 = 0x04;
pub const PAGING_STATE: u8 = 0x08;
pub const SERIAL_CONSISTENCY: u8 = 0x10;
pub const TIMESTAMP: u8 = 0x20;

pub struct Query(Vec<u8>);

impl Header for Query {
    fn new() -> Self {
        Query(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Query(Vec::with_capacity(capacity))
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
    fn length(mut self, length: i32) -> Self {
        self.0.extend(&i32::to_be_bytes(length));
        self
    }
}

impl Query {
    fn statement(mut self, statement: &str) -> Self {
        self.0.extend(&i32::to_be_bytes(statement.len() as i32));
        self.0.extend(statement.bytes());
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
}
