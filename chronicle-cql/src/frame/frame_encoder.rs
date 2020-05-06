use super::types::{
    Flags,
    Opcode,
    Version,
};
use rand;
use std::{
    collections::HashMap,
    convert::TryFrom,
};

type Short = u16; // A 2 bytes unsigned integer
const CQL_VERSION: &'static str = "CQL_VERSION";
const CQL_VERSION_VAL: &'static str = "3.0.0";
const COMPRESSION: &'static str = "COMPRESSION";

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
//
// CQL Frame Structure
//
// 0         8        16        24        32         40
// +---------+---------+---------+---------+---------+
// | version |  flags  |      stream       | opcode  |
// +---------+---------+---------+---------+---------+
// |                length                 |
// +---------+---------+---------+---------+
// |                                       |
// .            ...  body ...              .
// .                                       .
// .                                       .
// +----------------------------------------

// TODO: use the macro to encode
// TODO: remove the Frame struct if it is not necessary
macro_rules! frame_u8 {
    ($version:expr, $flags:expr, $stream:expr, $opcode:expr, $body_bytes:expr) => {{
        let mut v: Vec<u8> = Vec::new();
        let length = u32::try_from($body_bytes.len()).unwrap();
        v.push($version as u8);
        v.push($flags);
        v.extend_from_slice(&$stream.to_be_bytes());
        v.push($opcode as u8);
        v.extend_from_slice(&length.to_be_bytes());
        v.extend_from_slice(&$body_bytes);
        v
    }};
}

pub struct Frame {
    pub version: Version,
    pub flags: Flags,
    pub stream: Short,
    pub opcode: Opcode,
    pub body: Vec<u8>,
}

pub trait FrameEncoder {
    fn create_startup_frame(compression: &str) -> Vec<u8>;
    fn create_auth_response_frame(token_bytes: &Vec<u8>) -> Vec<u8>;
    fn create_options_frame() -> Vec<u8>;
    fn create_query_frame(&self) -> Vec<u8>;
    fn create_prepare_frame(query: &str) -> Vec<u8>;
    fn create_execute_frame(&self) -> Vec<u8>;
    fn create_batch_frame(&self) -> Vec<u8>;
    fn create_register_frame(&self) -> Vec<u8>;
}

impl FrameEncoder for Frame {
    fn create_startup_frame(compression: &str) -> Vec<u8> {
        let version = Version::Request;
        let flags: u8 = 0;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Startup;

        // Prepare the body part
        let mut map = HashMap::new();
        map.insert(CQL_VERSION, CQL_VERSION_VAL);
        map.insert(COMPRESSION, compression);
        let mut body_bytes = vec![];
        for (key, val) in map.iter() {
            // push key len
            body_bytes.extend_from_slice(&u32::try_from(key.len()).unwrap().to_be_bytes());
            // push key
            body_bytes.extend_from_slice(key.as_bytes());
            // push val len
            body_bytes.extend_from_slice(&u32::try_from(val.len()).unwrap().to_be_bytes());
            // push val
            body_bytes.extend_from_slice(val.as_bytes());
        }

        // Encode the frame as u8 vector
        frame_u8!(version, flags, stream, opcode, body_bytes)
    }

    fn create_auth_response_frame(token_bytes: &Vec<u8>) -> Vec<u8> {
        let version = Version::Request;
        let flags: u8 = 0;
        let stream = rand::random::<u16>();
        let opcode = Opcode::AuthResponse;

        // Encode the frame as u8 vector
        frame_u8!(version, flags, stream, opcode, token_bytes)
    }
    fn create_options_frame() -> Vec<u8> {
        let version = Version::Request;
        let flags: u8 = 0;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Options;

        // Encode the frame as u8 vector
        frame_u8!(version, flags, stream, opcode, vec![0; 0])
    }
    fn create_query_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_prepare_frame(query: &str) -> Vec<u8> {
        let version = Version::Request;
        let flags: u8 = 0;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Prepare;
        let body_bytes = query.as_bytes();

        // Encode the frame as u8 vector
        frame_u8!(version, flags, stream, opcode, body_bytes)
    }
    fn create_execute_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_batch_frame(&self) -> Vec<u8> {
        todo!()
    }
    fn create_register_frame(&self) -> Vec<u8> {
        todo!()
    }
}

#[cfg(test)]
// TODO: check the encodeded bits
mod tests {
    use super::*;

    #[test]
    fn test_create_startup_frame() {
        let compression = "test_compression";
        let _ = Frame::create_startup_frame(compression);
    }

    #[test]
    fn test_create_auth_response_frame() {
        let token_bytes = vec![1, 2, 3];
        let _ = Frame::create_auth_response_frame(&token_bytes);
    }
    #[test]
    fn test_create_options_frame() {
        let _ = Frame::create_options_frame();
    }

    #[test]
    fn test_create_prepare_frame() {
        let query = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";
        let _ = Frame::create_prepare_frame(query);
    }
}
