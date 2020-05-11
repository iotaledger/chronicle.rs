pub trait Compression {
    fn decompress(&self, buffer: &mut Vec<u8>);
    // todo fn compress()
}
pub const LZ4: Lz4 = Lz4;
pub struct Lz4;
impl Compression for Lz4 {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some lz4 crate;
    }
}

pub const SNAPPY: Snappy = Snappy;
pub struct Snappy;
impl Compression for Snappy {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some Snappy crate;
    }
}

pub const UNCOMPRESSED: Uncompressed = Uncompressed;
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn decompress(&self, _: &mut Vec<u8>) {unreachable!()}
}
