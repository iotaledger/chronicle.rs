use super::{encoder::BE_0_BYTES_LEN, header::Header, opcode::STARTUP};
use crate::compression::Compression;
use std::collections::HashMap;

pub struct Startup(Vec<u8>);

impl Header for Startup {
    fn new() -> Self {
        Startup(Vec::new())
    }
    fn with_capacity(capacity: usize) -> Self {
        Startup(Vec::with_capacity(capacity))
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
        self.0.push(STARTUP);
        self
    }
    fn length(mut self) -> Self {
        self.0.extend(&BE_0_BYTES_LEN);
        self
    }
}

impl Startup {
    pub fn options(mut self, map: &HashMap<&str, &str>) -> Self {
        self.0.extend(&u16::to_be_bytes(map.keys().len() as u16));
        for (k, v) in map {
            self.0.extend(&u16::to_be_bytes(k.len() as u16));
            self.0.extend(k.bytes());
            self.0.extend(&u16::to_be_bytes(v.len() as u16));
            self.0.extend(v.bytes());
        }
        let body_length = i32::to_be_bytes((self.0.len() as i32) - 9);
        self.0[5..9].copy_from_slice(&body_length);
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
    use crate::{compression::UNCOMPRESSED, frame::header};
    #[test]
    // note: junk data
    fn simple_startup_builder_test() {
        let mut options = HashMap::new();
        options.insert("CQL_VERSION", "3.0.0");

        let Startup(_payload) = Startup::new()
            .version()
            .flags(header::IGNORE)
            .stream(0)
            .opcode()
            .length()
            .options(&options)
            .build(UNCOMPRESSED); // build uncompressed
    }
}
