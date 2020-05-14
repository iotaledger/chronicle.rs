use lz4;
use snap;

pub trait Compression {
    fn decompress(&self, buffer: &mut Vec<u8>);
    fn compress(&self, buffer: &mut Vec<u8>);
}
pub const LZ4: Lz4 = Lz4;
pub struct Lz4;
impl Compression for Lz4 {
    fn decompress(&self, buffer: &mut Vec<u8>) {
        // Get the compressed body length
        let mut length_array: [u8; 4] = [0, 0, 0, 0];
        length_array.copy_from_slice(&buffer[5..9]);
        let compressed_body_length = i32::from_be_bytes(length_array) as usize;

        // Decompress the body by lz4
        let decompressed_buffer: Vec<u8> =
            lz4::block::decompress(&buffer[13..(13 + compressed_body_length)], None).unwrap();

        // Truncate the buffer to be header, followed by the first 4 bytes of the body
        buffer.truncate(13);

        // Replace the body length w/ the decompressed body length
        let mut decompressed_length_array: [u8; 4] = [0, 0, 0, 0];
        decompressed_length_array.copy_from_slice(&buffer[9..13]);
        buffer[5..9].copy_from_slice(&decompressed_length_array);

        // Extend the decompressed body
        buffer.extend(&decompressed_buffer)
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // Get the body length
        let mut length_array: [u8; 4] = [0, 0, 0, 0];
        length_array.copy_from_slice(&buffer[5..9]);
        let body_length = i32::from_be_bytes(length_array) as usize;

        // Compress the body
        let compressed_buffer: Vec<u8> = lz4::block::compress(&buffer[9..(9 + body_length)], None, true).unwrap();

        // Truncate the buffer to be header only
        buffer.truncate(9);

        // The first 4 bytes of the body is the uncompressed length
        buffer.extend(length_array.iter().copied());

        // Update the body length to be the compressed body length
        let compressed_body_length = i32::to_be_bytes(compressed_buffer.len() as i32);
        buffer[5..9].copy_from_slice(&compressed_body_length);

        // Extend the compressed body
        buffer.extend(&compressed_buffer)
    }
}

pub const SNAPPY: Snappy = Snappy;
pub struct Snappy;
impl Compression for Snappy {
    fn decompress(&self, buffer: &mut Vec<u8>) {
        // Get the compressed body length
        let mut length_array: [u8; 4] = [0, 0, 0, 0];
        length_array.copy_from_slice(&buffer[5..9]);
        let compressed_body_length = i32::from_be_bytes(length_array) as usize;

        // Decompress the body by snappy
        let decompressed_buffer: Vec<u8> = snap::raw::Decoder::new()
            .decompress_vec(&buffer[9..(9 + compressed_body_length)])
            .unwrap();

        // Truncate the buffer to be header only
        buffer.truncate(9);

        // Update the compressed body length to be the body length
        let body_length = i32::to_be_bytes(decompressed_buffer.len() as i32);
        buffer[5..9].copy_from_slice(&body_length);

        // Extend the body
        buffer.extend(&decompressed_buffer)
    }
    fn compress(&self, buffer: &mut Vec<u8>) {
        // Get the body length
        let mut length_array: [u8; 4] = [0, 0, 0, 0];
        length_array.copy_from_slice(&buffer[5..9]);
        let body_length = i32::from_be_bytes(length_array) as usize;

        // Compress the body
        let compressed_buffer: Vec<u8> = snap::raw::Encoder::new()
            .compress_vec(&buffer[9..(9 + body_length)])
            .unwrap();

        // Truncate the buffer to be header only
        buffer.truncate(9);

        // Update the body length to be the compressed body length
        let compressed_body_length = i32::to_be_bytes(compressed_buffer.len() as i32);
        buffer[5..9].copy_from_slice(&compressed_body_length);

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
