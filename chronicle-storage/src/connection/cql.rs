use crate::{
    cluster::supervisor::Tokens as ClusterTokens,
    node::supervisor::gen_node_id,
    ring::{
        Msb,
        ShardCount,
        DC,
    },
};
use chronicle_cql::frame::{
    auth_challenge::AuthChallenge,
    auth_response::{
        AuthResponse,
        Authenticator,
    },
    authenticate::Authenticate,
    decoder::{
        Decoder,
        Frame,
    },
    header::Header,
    *,
};

use log::*;

use std::{
    collections::HashMap,
    i64,
    net::Ipv4Addr,
};
use tokio::{
    io::{
        Error,
        ErrorKind,
    },
    net::TcpStream,
};

use chronicle_cql::{
    compression::MyCompression,
    frame::{
        consistency::Consistency,
        decoder::ColumnDecoder,
        query::Query,
        queryflags::SKIP_METADATA,
    },
    rows,
};

pub type Address = String;

use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

#[derive(Debug)]
pub struct CqlConn {
    stream: Option<TcpStream>,
    tokens: Option<ClusterTokens>,
    shard_id: u8,
    shard_count: ShardCount,
    msb: Msb,
    dc: Option<String>,
}

impl CqlConn {
    pub fn get_shard_count(&self) -> ShardCount {
        self.shard_count
    }
    pub fn take_tokens(&mut self) -> ClusterTokens {
        self.tokens.take().unwrap()
    }
    pub fn take_stream(&mut self) -> TcpStream {
        self.stream.take().unwrap()
    }
    pub fn take_dc(&mut self) -> DC {
        self.dc.take().unwrap()
    }
}

// ----------- decoding scope -----------
rows!(
    rows: Info {data_center: String, broadcast_address: Ipv4Addr, tokens: Vec<String>},
    row: Row(
        DataCenter,
        BroadcastAddress,
        Tokens
    ),
    column_decoder: InfoDecoder
);

pub trait Rows {
    fn decode(self) -> Self;
    fn finalize(self) -> Self;
}

impl Rows for Info {
    fn decode(mut self) -> Self {
        while let Some(_) = self.next() {}
        self
    }
    fn finalize(self) -> Self {
        self
    }
}

impl InfoDecoder for DataCenter {
    fn decode_column(start: usize, length: i32, acc: &mut Info) {
        acc.data_center = String::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Info) {
        unreachable!()
    }
}

impl InfoDecoder for BroadcastAddress {
    fn decode_column(start: usize, length: i32, acc: &mut Info) {
        acc.broadcast_address = Ipv4Addr::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Info) {
        unreachable!()
    }
}

impl InfoDecoder for Tokens {
    fn decode_column(start: usize, length: i32, acc: &mut Info) {
        acc.tokens = Vec::decode(&acc.buffer()[start..], length as usize);
    }
    fn handle_null(_: &mut Info) {
        unreachable!()
    }
}

pub async fn connect(
    address: &str,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    authenticator: Option<&impl Authenticator>,
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
    // create options frame
    let options = options::Options::new()
        .version()
        .flags(0)
        .stream(0)
        .opcode()
        .length()
        .build();
    // write_all options frame to stream
    stream.write_all(&options.0).await?;
    // get_frame_payload
    let buffer = get_frame_payload(&mut stream).await?;
    // Create Decoder from buffer
    let decoder = Decoder::new(buffer, MyCompression::get());
    if decoder.is_error() {
        // check if response is_error.
        error!("{:?}", decoder.get_error());
        return Err(Error::new(
            ErrorKind::Other,
            "CQL connection not supported due to CqlError",
        ));
    }
    assert!(decoder.is_supported());
    // decode supported options from decoder
    let supported = supported::Supported::new(&decoder);
    // create empty hashmap options;
    let mut options: HashMap<String, String> = HashMap::new();
    // get the supported_cql_version option;
    let cql_version = supported.get_options().get("CQL_VERSION").unwrap().first().unwrap();
    // insert the supported_cql_version option into the options;
    options.insert("CQL_VERSION".to_owned(), cql_version.to_owned());
    // insert the supported_compression option into the options if it was set.;
    if let Some(compression) = MyCompression::option() {
        options.insert("COMPRESSION".to_owned(), compression.to_owned());
    }
    // create startup frame using the selected options;
    let startup = startup::Startup::new()
        .version()
        .flags(0)
        .stream(0)
        .opcode()
        .length()
        .options(&options);
    // write_all startup frame to stream;
    stream.write_all(&startup.0).await?;
    // get_frame_payload
    let buffer = get_frame_payload(&mut stream).await?;
    // Create Decoder from buffer.
    let decoder = Decoder::new(buffer, MyCompression::get());
    if decoder.is_authenticate() {
        if authenticator.is_none() {
            let authenticate = Authenticate::new(&decoder);
            error!("Authenticator: {:?} is not provided", authenticate.authenticator());
            return Err(Error::new(
                ErrorKind::Other,
                "CQL connection not ready due to authenticator is not provided",
            ));
        }
        let auth_response = AuthResponse::new()
            .version()
            .flags(MyCompression::flag())
            .stream(0)
            .opcode()
            .length()
            .token(authenticator.as_ref().unwrap().clone())
            .build(MyCompression::get());
        // write_all auth_response frame to stream;
        stream.write_all(&auth_response.0).await?;
        // get_frame_payload
        let buffer = get_frame_payload(&mut stream).await?;
        // Create Decoder from buffer.
        let decoder = Decoder::new(buffer, MyCompression::get());
        if decoder.is_error() {
            error!("Unable to authenticate: {:?}", decoder.get_error());
            return Err(Error::new(ErrorKind::Other, "CQL connection not ready due to CqlError"));
        }
        if decoder.is_auth_challenge() {
            let auth_challenge = AuthChallenge::new(&decoder);
            error!("Unsupported auth_challenge {:?}", auth_challenge);
            return Err(Error::new(
                ErrorKind::Other,
                "CQL connection not ready due to Auth Challenge",
            ));
        }
        assert!(decoder.is_auth_success());
    } else if decoder.is_error() {
        error!("{:?}", decoder.get_error());
        return Err(Error::new(ErrorKind::Other, "CQL connection not ready due to CqlError"));
    } else {
        assert!(decoder.is_ready());
    }
    // copy usefull options
    let shard = supported.get_options().get("SCYLLA_SHARD").unwrap()[0].parse().unwrap();
    let nr_shard = supported.get_options().get("SCYLLA_NR_SHARDS").unwrap()[0]
        .parse()
        .unwrap();
    let ignore_msb = supported.get_options().get("SCYLLA_SHARDING_IGNORE_MSB").unwrap()[0]
        .parse()
        .unwrap();
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
    // create query to fetch tokens and info from system.local;
    let query = query();
    // write_all query to the stream
    cqlconn.stream.as_mut().unwrap().write_all(query.as_slice()).await?;
    // get_frame_payload
    let buffer = get_frame_payload(cqlconn.stream.as_mut().unwrap()).await?;
    // Create Decoder from buffer.
    let decoder = Decoder::new(buffer, MyCompression::get());
    let info;
    if decoder.is_rows() {
        let data_center = String::new();
        let broadcast_address = Ipv4Addr::new(0, 0, 0, 0);
        let tokens = Vec::new();
        info = Info::new(decoder, data_center, broadcast_address, tokens)
            .decode()
            .finalize();
    } else {
        error!("{:?}", decoder.get_error());
        return Err(Error::new(ErrorKind::Other, "CQL connection not rows due to CqlError"));
    }
    let mut cluster_tokens: ClusterTokens = Vec::new();
    for token in info.tokens.iter() {
        let node_id = gen_node_id(&info.broadcast_address.to_string());
        let token = i64::from_str_radix(token, 10).unwrap();
        cluster_tokens.push((
            token,
            node_id,
            info.data_center.clone(),
            cqlconn.msb,
            cqlconn.shard_count,
        ))
    }
    cqlconn.tokens.replace(cluster_tokens);
    cqlconn.dc.replace(info.data_center);
    Ok(cqlconn)
}

pub async fn connect_to_shard_id(
    address: &str,
    shard_id: u8,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    authenticator: Option<&impl Authenticator>,
) -> Result<CqlConn, Error> {
    // buffer connections temporary to force scylla connects us to new shard_id
    let mut conns = Vec::new();
    // loop till we connect to the right shard_id
    loop {
        match connect(address, recv_buffer_size, send_buffer_size, authenticator).await {
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

async fn get_frame_payload(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    // create buffer
    let mut buffer = vec![0; 9];
    // read response into buffer
    stream.read_exact(&mut buffer).await?;
    let body_length = i32::from_be_bytes(buffer[5..9].try_into().unwrap());
    // extend buffer
    buffer.resize((body_length + 9).try_into().unwrap(), 0);
    stream.read_exact(&mut buffer[9..]).await?;
    Ok(buffer)
}

// ----------- encoding scope -----------
pub fn query() -> Vec<u8> {
    let Query(payload) = Query::new()
        .version()
        .flags(MyCompression::flag())
        .stream(0)
        .opcode()
        .length()
        .statement("SELECT data_center, broadcast_address, tokens FROM system.local")
        .consistency(Consistency::One)
        .query_flags(SKIP_METADATA)
        .build(MyCompression::get());
    payload
}
