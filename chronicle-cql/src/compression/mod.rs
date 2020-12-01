// Copyright 2020 IOTA Stiftung
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//! This crates implements the uncompressed, LZ4, and snappy compression methods for Cassandra.

use crate::frame::header;
use std::convert::TryInto;

/// This compression thread provides the buffer compression/decompression methods for uncompressed/Lz4/snappy.
pub trait Compression: Sync {
    /// The compression type string, `lz4` or `snappy` or None.
    fn option(&self) -> Option<&'static str>;
    /// Decompress buffer only if compression flag is set
    fn decompress(&self, compressed: Vec<u8>) -> Vec<u8>;
    /// Compression the buffer according to the compression type (Lz4 for snappy).
    fn compress(&self, uncompressed: Vec<u8>) -> Vec<u8>;
}

/// LZ4 compression type.
pub const LZ4: Lz4 = Lz4;
/// LZ4 unit structure which implements compression trait.
pub struct Lz4;

impl Compression for Lz4 {
    fn option(&self) -> Option<&'static str> {
        Some("lz4")
    }
    fn decompress(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        // check if buffer is compressed
        if buffer[1] & header::COMPRESSION == header::COMPRESSION {
            let compressed_body_length = i32::from_be_bytes(buffer[5..9].try_into().unwrap()) as usize;
            // Decompress the body by lz4
            let decompressed_buffer: Vec<u8> = lz4::block::decompress(&buffer[13..(13 + compressed_body_length)], None).unwrap();
            // adjust the length to equal the uncompressed length
            buffer.copy_within(9..13, 5);
            // reduce the frame to be a header only
            buffer.truncate(9);
            // Extend the decompressed body
            buffer.extend(&decompressed_buffer);
            buffer
        } else {
            // return the buffer as it's
            buffer
        }
    }
    fn compress(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        // Compress the body
        let compressed_buffer: Vec<u8> = lz4::block::compress(&buffer[9..], None, true).unwrap();
        // Truncate the buffer to be header without length
        buffer.truncate(5);
        // make the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer);
        buffer
    }
}

/// SNAPPY compression type.
pub const SNAPPY: Snappy = Snappy;
/// Snappy unit structure which implements compression trait.
pub struct Snappy;
impl Compression for Snappy {
    fn option(&self) -> Option<&'static str> {
        Some("snappy")
    }
    fn decompress(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        if buffer[1] & header::COMPRESSION == header::COMPRESSION {
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
            buffer.extend(&decompressed_buffer);
            buffer
        } else {
            buffer
        }
    }
    fn compress(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        // Compress the body
        let compressed_buffer: Vec<u8> = snap::raw::Encoder::new().compress_vec(&buffer[9..]).unwrap();
        // Truncate the buffer to be header only without length
        buffer.truncate(5);
        // Update the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer);
        buffer
    }
}

/// Uncompresed type.
pub const UNCOMPRESSED: Uncompressed = Uncompressed;
/// Uncompressed unit structure which implements compression trait.
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn option(&self) -> Option<&'static str> {
        None
    }
    fn decompress(&self, buffer: Vec<u8>) -> Vec<u8> {
        buffer
    }
    fn compress(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        // no need to compress, only adjust the body length
        let body_length = i32::to_be_bytes((buffer.len() as i32) - 9);
        buffer[5..9].copy_from_slice(&body_length);
        buffer
    }
}
/// `MY_COMPRESSION` is used to enable user defines a global compression structure.
pub static mut MY_COMPRESSION: MyCompression = MyCompression(&UNCOMPRESSED);
/// `MY_COMPRESSION_FLAG` is used to indicate whether the compression is applied to the buffer.
pub static mut MY_COMPRESSION_FLAG: u8 = 0;
#[derive(Copy, Clone)]
/// `MyCompression` structure provides a higher-level wrapper to let the user use a compresion method, i.e.,
/// ````LZ4`, `SNAPPY`, or `UNCOMPRESSED`.
pub struct MyCompression(pub &'static dyn Compression);

impl MyCompression {
    /// Set the global compression syte as `LZ4`.
    pub fn set_lz4() {
        unsafe {
            MY_COMPRESSION = MyCompression(&LZ4);
            MY_COMPRESSION_FLAG = 1;
        }
    }
    /// Set the global compression syte as `SNAPPY`.
    pub fn set_snappy() {
        unsafe {
            MY_COMPRESSION = MyCompression(&SNAPPY);
            MY_COMPRESSION_FLAG = 1;
        }
    }
    /// Set the global compression syte as `UNCOMPRESSED`.
    pub fn set_uncompressed() {
        unsafe {
            MY_COMPRESSION = MyCompression(&UNCOMPRESSED);
            MY_COMPRESSION_FLAG = 0;
        }
    }
    /// Get the global structure, `MY_COMPRESSION`.
    pub fn get() -> impl Compression {
        unsafe { MY_COMPRESSION }
    }
    /// Get the global structure, `MY_COMPRESSION_FLAG`.
    pub fn flag() -> u8 {
        unsafe { MY_COMPRESSION_FLAG }
    }
    /// Get the `Option` of compression method type, i.e., `lz4`, `snappy`, or None.
    pub fn option() -> Option<&'static str> {
        unsafe { MY_COMPRESSION }.option()
    }
}

impl Compression for MyCompression {
    fn option(&self) -> Option<&'static str> {
        self.0.option()
    }
    fn decompress(&self, buffer: Vec<u8>) -> Vec<u8> {
        // get the inner compression and then decompress
        self.0.decompress(buffer)
    }
    fn compress(&self, buffer: Vec<u8>) -> Vec<u8> {
        // get the inner compression and then compress
        self.0.compress(buffer)
    }
}
