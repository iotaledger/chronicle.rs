use super::{
    encoder::BE_0_BYTES_LEN,
    header::Header,
    opcode::PREPARE,
};

use crate::compression::Compression;

pub struct Prepare(Vec<u8>);

impl Header for Prepare {
    fn new() -> Self {
        Prepare(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Prepare(Vec::with_capacity(capacity))
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
        self.0.push(PREPARE);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Prepare {
    pub fn statement(mut self, statement: &str) -> Self {
        self.0.extend(&i32::to_be_bytes(statement.len() as i32));
        self.0.extend(statement.bytes());
        self
    }
    pub fn build(mut self, compression: impl Compression) -> Self {
        self.0 = compression.compress(self.0);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compression::UNCOMPRESSED,
        frame::header,
        statements::statements::INSERT_TX_QUERY,
    };
    #[test]
    // note: junk data
    fn simple_prepare_builder_test() {
        let Prepare(_payload) = Prepare::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode()
            .length()
            .statement(INSERT_TX_QUERY)
            .build(UNCOMPRESSED); // build uncompressed
    }
}
