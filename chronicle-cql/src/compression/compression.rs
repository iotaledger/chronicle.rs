pub trait Compression {
    fn decompress(&self, buffer: &mut Vec<u8>);
}
pub const LZ4: Option<Lz4> = Some(Lz4);
pub struct Lz4;
impl Compression for Lz4 {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some lz4 crate;
    }
}
impl Lz4 {
    pub fn new() -> Option<Self> {
        LZ4
    }
}
pub const SNAPPY: Option<Snappy> = Some(Snappy);
pub struct Snappy;
impl Compression for Snappy {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some Snappy crate;
    }
}
impl Snappy {
    pub fn new() -> Option<Self> {
        SNAPPY
    }
}
pub const UNCOMPRESSED: Option<Uncompressed> = None;
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn decompress(&self, _: &mut Vec<u8>) {unreachable!()}
}
impl Uncompressed {
    pub fn new() -> Option<Self> {
        UNCOMPRESSED
    }
}
