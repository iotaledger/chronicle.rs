pub trait Compression {
    fn decompress(&self, buffer: &mut Vec<u8>);
    fn compress(&self, buffer: &mut Vec<u8>);
}
pub const LZ4: Lz4 = Lz4;
pub struct Lz4;
impl Compression for Lz4 {
    fn decompress(&self, _buffer: &mut Vec<u8>) {
        // TODO decompress using some lz4 crate;
        // note: make sure to adjust the body length
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // TODO compress using some lz4 crate;
        // note: make sure to adjust the body length
    }
}

pub const SNAPPY: Snappy = Snappy;
pub struct Snappy;
impl Compression for Snappy {
    fn decompress(&self, _buffer: &mut Vec<u8>) {
        // TODO decompress using some Snappy crate;
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // TODO compress using some snappy crate;
        // note: make sure to adjust the body length
    }
}

pub const UNCOMPRESSED: Uncompressed = Uncompressed;
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn decompress(&self, _: &mut Vec<u8>) {unreachable!()}
    fn compress(&self, buffer: &mut Vec<u8>) {
        // no need to compress, only adjust the body length
        let body_length = i32::to_be_bytes((buffer.len() as i32) -9);
        buffer[5..9].copy_from_slice(&body_length)
    }
}
