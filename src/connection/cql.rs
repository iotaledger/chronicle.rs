use crate::ring::ring::{Msb, ShardCount};
use tokio::net::TcpStream;
use crate::cluster::supervisor::Tokens;

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
pub struct Error; // todo: change it to tokio error

pub async fn connect(address: &Address) -> Result<CqlConn, Error> {
    // connect using tokio and return

    // establish cql using startup frame and ensure is ready

    // send options frame and decode supported frame as options

    // create cqlconn
    todo!()
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
                    break Ok(cqlconn)
                } else if shard_id >= cqlconn.shard_count {
                    // error as it's impossible to connect to shard_id doesn't exist
                    break Err(Error)
                } else {
                    // continue to retry
                    continue
                }
            }
            err => {
                break err;
            }
        }
    }

}
