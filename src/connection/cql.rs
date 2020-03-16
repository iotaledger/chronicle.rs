use crate::cluster::supervisor::Tokens;
use crate::ring::ring::{Msb, NodeId, ShardCount, Token, DC};
use byteorder::{BigEndian, ByteOrder};
use cdrs::compression::Compression;
use cdrs::frame::traits::FromCursor;
use cdrs::frame::Frame;
use cdrs::frame::Opcode;
use cdrs::frame::{frame_supported, IntoBytes};
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::Error;
use tokio::io::ErrorKind;
use tokio::net::TcpStream;
use tokio::prelude::*;

pub type Address = String;

pub struct CqlConn {
    stream: Option<TcpStream>,
    tokens: Option<Tokens>,
    shard_id: u8,
    shard_count: ShardCount,
    msb: Msb,
}

impl CqlConn {
    pub fn get_shard_count(&self) -> ShardCount {
        self.shard_count
    }
    pub fn take_tokens(&mut self) -> Tokens {
        self.tokens.take().unwrap()
    }
    pub fn take_stream(&mut self) -> TcpStream {
        self.stream.take().unwrap()
    }
}

pub async fn connect(address: &Address) -> Result<CqlConn, Error> {
    // connect using tokio and return
    let mut stream = TcpStream::connect(address.clone()).await?;
    // establish cql using startup frame and ensure is ready
    let mut shard: u8 = 0;
    let mut nr_shard: u8 = 0;
    let mut ignore_msb: u8 = 0;
    {
        let stream_copy = &mut stream;
        let shard_copy = &mut shard;
        let nr_shard_copy = &mut nr_shard;
        let ignore_msb_copy = &mut ignore_msb;
        let (mut socket_rx, mut socket_tx) = tokio::io::split(stream_copy);
        let ref mut compression = Compression::None;
        let startup_frame = Frame::new_req_startup(compression.as_str()).into_cbytes();
        socket_tx.write(startup_frame.as_slice()).await?;
        let mut ready_buffer = vec![0; 9];
        socket_rx.read(&mut ready_buffer).await?;
        if Opcode::from(ready_buffer[4]) != Opcode::Ready {
            return Err(Error::new(ErrorKind::Other, "CQL connection failed."));
        }
        // send options frame and decode supported frame as options
        let option_frame = Frame::new_req_options().into_cbytes();
        socket_tx.write(option_frame.as_slice()).await?;
        let mut head_buffer = vec![0; 9];
        socket_rx.read(&mut head_buffer).await?;
        let length = BigEndian::read_u32(&head_buffer[5..9]) as usize;
        let mut body_buffer = vec![0; length];
        socket_rx.read(&mut body_buffer).await?;
        let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
        let options = frame_supported::BodyResSupported::from_cursor(&mut cursor)
            .unwrap()
            .data;
        *shard_copy = options.get("SCYLLA_SHARD").unwrap()[0].parse().unwrap();
        *nr_shard_copy = options.get("SCYLLA_NR_SHARDS").unwrap()[0].parse().unwrap();
        *ignore_msb_copy = options.get("SCYLLA_SHARDING_IGNORE_MSB").unwrap()[0]
            .parse()
            .unwrap();
    }

    // create cqlconn
    // TDO: Get the tokens
    let cqlconn = CqlConn {
        stream: Some(stream),
        tokens: None,
        shard_id: shard,
        shard_count: nr_shard,
        msb: ignore_msb,
    };
    Ok(cqlconn)
}
pub async fn fetch_tokens(connection: Result<CqlConn, Error>) -> Result<CqlConn, Error> {
    // fetch tokens from scylla using select query to system.local table,
    // then add it to cqlconn
    todo!()
}

pub async fn connect_to_shard_id(address: &Address, shard_id: u8) -> Result<CqlConn, Error> {
    // loop till we connect to the right shard_id
    loop {
        match connect(address).await {
            Ok(cqlconn) => {
                if cqlconn.shard_id == shard_id {
                    // return
                    break Ok(cqlconn);
                } else if shard_id >= cqlconn.shard_count {
                    // error as it's impossible to connect to shard_id doesn't exist
                    break Err(Error::new(ErrorKind::Other, "shard_id does not exist."));
                } else {
                    // continue to retry
                    continue;
                }
            }
            err => {
                break err;
            }
        }
    }
}
