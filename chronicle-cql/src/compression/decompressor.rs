pub trait Decompressor {
    fn decompress(&self, buffer: &mut Vec<u8>);
}

pub struct Lz4;
impl Decompressor for Lz4 {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some lz4 crate;
    }
}

pub struct Snappy;
impl Decompressor for Snappy {
    fn decompress(&self, _buffer: &mut std::vec::Vec<u8>) {
        // TODO decompress using some Snappy crate;
    }
}

pub struct Uncompressed;
impl Decompressor for Uncompressed {
    fn decompress(&self, _: &mut Vec<u8>) {}
}
impl Uncompressed {
    pub fn new() -> Option<Uncompressed> {
        None
    }
}
