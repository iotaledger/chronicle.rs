use lz4;
use snap;
use std::convert::TryInto;

pub trait Compression {
    fn decompress(&self, buffer: &mut Vec<u8>);
    fn compress(&self, buffer: &mut Vec<u8>);
}
pub const LZ4: Lz4 = Lz4;
pub struct Lz4;
impl Compression for Lz4 {
    fn decompress(&self, buffer: &mut Vec<u8>) {
        let compressed_body_length = i32::from_be_bytes(buffer[5..9].try_into().unwrap()) as usize;
        // Decompress the body by lz4
        let decompressed_buffer: Vec<u8> =
            lz4::block::decompress(&buffer[13..(13+compressed_body_length)], None).unwrap();
        // adjust the length to equal the uncompressed length
        buffer.copy_within(9..13, 5);
        // reduce the frame to be a header only
        buffer.truncate(9);
        // Extend the decompressed body
        buffer.extend(&decompressed_buffer)
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // Compress the body
        let compressed_buffer: Vec<u8> = lz4::block::compress(&buffer[9..], None, true).unwrap();
        // Truncate the buffer to be header without length
        buffer.truncate(5);
        // make the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer)
    }
}

pub const SNAPPY: Snappy = Snappy;
pub struct Snappy;
impl Compression for Snappy {
    fn decompress(&self, buffer: &mut Vec<u8>) {
        let compressed_body_length = i32::from_be_bytes(buffer[5..9].try_into().unwrap()) as usize;
        // Decompress the body by snappy
        let decompressed_buffer: Vec<u8> = snap::raw::Decoder::new()
             .decompress_vec(&buffer[9..(9 + compressed_body_length)])
             .unwrap();
        // reduce the frame to be a header only without length
        buffer.truncate(5);
        // make the body length to be the decompressed body length
        buffer.extend(&i32::to_be_bytes(decompressed_buffer.len() as i32));
        // Extend the decompressed body
        buffer.extend(&decompressed_buffer)
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // Compress the body
        let compressed_buffer: Vec<u8> = snap::raw::Encoder::new()
            .compress_vec(&buffer[9..])
            .unwrap();
        // Truncate the buffer to be header only without length
        buffer.truncate(5);
        // Update the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer)
    }
}

pub const UNCOMPRESSED: Uncompressed = Uncompressed;
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn decompress(&self, _: &mut Vec<u8>) {
        unreachable!()
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // no need to compress, only adjust the body length
        let body_length = i32::to_be_bytes((buffer.len() as i32) - 9);
        buffer[5..9].copy_from_slice(&body_length)
    }
}
