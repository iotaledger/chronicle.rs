use super::{
    encoder::BE_0_BYTES_LEN,
    header::Header,
};

pub struct Options(Vec<u8>);

impl Header for Options {
    fn new() -> Self {
        Options(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Options(Vec::with_capacity(capacity))
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

impl Options {
    fn build(mut self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{
        header,
        opcode::OPTIONS,
    };

    #[test]
    // note: junk data
    fn simple_options_builder_test() {
        let Options(_payload) = Options::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode(OPTIONS)
            .length()
            .build(); // build uncompressed
    }
}
