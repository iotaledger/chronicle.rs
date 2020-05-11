pub trait Header {
    fn new() -> Self;
    fn with_capacity(capacity: usize) -> Self;
    fn version(self) -> Self;
    fn flags(self, flags: u8) -> Self;
    fn stream(self, stream: i16) -> Self;
    fn length(self, length: i32) -> Self;
}
