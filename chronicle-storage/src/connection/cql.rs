use crate::{
    cluster::supervisor::Tokens,
    node::supervisor::gen_node_id,
    ring::{
        Msb,
        ShardCount,
        DC,
    },
};
use cdrs::{
    compression::Compression,
    frame::{
        frame_result,
        frame_supported,
        traits::FromCursor,
        Flag,
        Frame,
        IntoBytes,
        Opcode,
        TryFromRow,
    },
    query,
    types::{
        from_cdrs::FromCDRSByName,
        prelude::{
            Bytes,
            List,
            Row,
            Value,
        },
        AsRustType,
    },
};
use std::{
    i64,
    io::Cursor,
    net::IpAddr,
};
use tokio::{
    io::{
        Error,
        ErrorKind,
    },
    net::TcpStream,
    prelude::*,
};

use cdrs_helpers_derive::{
    IntoCDRSValue,
    TryFromRow,
};

pub type Address = String;

#[derive(Debug)]
pub struct CqlConn {
    stream: Option<TcpStream>,
    tokens: Option<Tokens>,
    shard_id: u8,
    shard_count: ShardCount,
    msb: Msb,
    dc: Option<String>,
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
    pub fn take_dc(&mut self) -> DC {
        self.dc.take().unwrap()
    }
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowTokens {
    data_center: String,
    rpc_address: IpAddr,
    tokens: Vec<String>,
}

pub async fn connect(
    address: &str,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
) -> Result<CqlConn, Error> {
    // connect using tokio and return
    let mut stream = TcpStream::connect(address).await?;
    // set socket flags
    if let Some(recv_buffer_size) = recv_buffer_size {
        stream.set_recv_buffer_size(recv_buffer_size)?
    }
    if let Some(send_buffer_size) = send_buffer_size {
        stream.set_send_buffer_size(send_buffer_size)?
    }
    // establish cql using startup frame and ensure is ready
    let compression = &mut Compression::None;
    let startup_frame = Frame::new_req_startup(compression.as_str()).into_cbytes();
    stream.write(startup_frame.as_slice()).await?;
    let mut ready_buffer = vec![0; 9];
    stream.read(&mut ready_buffer).await?;
    if Opcode::from(ready_buffer[4]) != Opcode::Ready {
        return Err(Error::new(ErrorKind::Other, "CQL connection failed."));
    }
    // send options frame and decode supported frame as options
    let option_frame = Frame::new_req_options().into_cbytes();
    stream.write(option_frame.as_slice()).await?;
    let mut head_buffer = vec![0; 9];
    stream.read(&mut head_buffer).await?;
    let length = get_body_length_usize(&head_buffer);
    let mut body_buffer = vec![0; length];
    stream.read(&mut body_buffer).await?;
    let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
    let options = frame_supported::BodyResSupported::from_cursor(&mut cursor)
        .unwrap()
        .data;
    let shard = options.get("SCYLLA_SHARD").unwrap()[0].parse().unwrap();
    let nr_shard = options.get("SCYLLA_NR_SHARDS").unwrap()[0].parse().unwrap();
    let ignore_msb = options.get("SCYLLA_SHARDING_IGNORE_MSB").unwrap()[0].parse().unwrap();
    // create cqlconn
    let cqlconn = CqlConn {
        stream: Some(stream),
        tokens: None,
        shard_id: shard,
        shard_count: nr_shard,
        msb: ignore_msb,
        dc: None,
    };
    Ok(cqlconn)
}
pub async fn fetch_tokens(connection: Result<CqlConn, Error>) -> Result<CqlConn, Error> {
    let mut cqlconn = connection?;
    // fetch tokens from scylla using select query to system.local table,
    // then add it to cqlconn
    // query param builder
    let params = query::QueryParamsBuilder::new().page_size(500).finalize();
    let query = "SELECT data_center, rpc_address, tokens FROM system.local;".to_string();
    let query = query::Query { query, params };
    // query_frame
    let query_frame = Frame::new_query(query, vec![Flag::Ignore]).into_cbytes();
    // write frame to stream
    cqlconn.stream.as_mut().unwrap().write(query_frame.as_slice()).await?;
    // read buffer
    let mut head_buffer = vec![0; 9];
    cqlconn.stream.as_mut().unwrap().read(&mut head_buffer).await?;
    let length = get_body_length_usize(&head_buffer);
    let mut body_buffer = vec![0; length];
    cqlconn.stream.as_mut().unwrap().read(&mut body_buffer).await?;
    let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
    let mut rows = frame_result::ResResultBody::from_cursor(&mut cursor)
        .unwrap()
        .into_rows()
        .unwrap();
    let row = RowTokens::try_from_row(rows.pop().unwrap()).unwrap();
    let rpc_address = row.rpc_address.to_string();
    let mut tokens: Tokens = Vec::new();
    for token in row.tokens.iter() {
        let node_id = gen_node_id(&rpc_address);
        let token = i64::from_str_radix(token, 10).unwrap();
        tokens.push((
            token,
            node_id,
            row.data_center.clone(),
            cqlconn.msb,
            cqlconn.shard_count,
        ))
    }
    cqlconn.tokens.replace(tokens);
    cqlconn.dc.replace(row.data_center);
    Ok(cqlconn)
}

pub async fn connect_to_shard_id(
    address: &str,
    shard_id: u8,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
) -> Result<CqlConn, Error> {
    // buffer connections temporary to force scylla connects us to new shard_id
    let mut conns = Vec::new();
    // loop till we connect to the right shard_id
    loop {
        match connect(address, recv_buffer_size, send_buffer_size).await {
            Ok(cqlconn) => {
                if cqlconn.shard_id == shard_id {
                    // return
                    break Ok(cqlconn);
                } else if shard_id >= cqlconn.shard_count {
                    // error as it's impossible to connect to shard_id doesn't exist
                    break Err(Error::new(ErrorKind::Other, "shard_id does not exist."));
                } else {
                    if conns.len() > cqlconn.shard_count as usize {
                        // clear conns otherwise we are going to overflow the memory
                        conns.clear();
                    }
                    conns.push(cqlconn);
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

pub fn get_body_length_usize(buffer: &[u8]) -> usize {
    ((buffer[5] as usize) << 24) + ((buffer[6] as usize) << 16) + ((buffer[7] as usize) << 8) + (buffer[8] as usize)
}
