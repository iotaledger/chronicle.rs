pub const IGNORE: u8 = 0x00;
pub const COMPRESSION: u8 = 0x01;
pub const TRACING: u8 = 0x02;
pub const CUSTOM_PAYLOAD: u8 = 0x04;
pub const WARNING: u8 = 0x08;

pub trait Header {
    fn new() -> Self;
    fn with_capacity(capacity: usize) -> Self;
    fn version(self) -> Self;
    fn flags(self, flags: u8) -> Self;
    fn stream(self, stream: i16) -> Self;
    fn opcode(self) -> Self;
    fn length(self) -> Self;
}
