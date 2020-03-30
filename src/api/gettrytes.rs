use crate::connection::cql::get_body_length_usize;

use crate::statements::*;
// use crate::worker::{Error, Status, StreamStatus, Worker};
use bee_bundle::{
    Address, Hash, Index, Nonce, Payload, Tag, Timestamp, Transaction, TransactionBuilder, Value,
};
use cdrs::frame::traits::{FromCursor, TryFromRow};
use cdrs::frame::{frame_result, frame_supported, Flag, Frame, IntoBytes};
use cdrs::types::{blob::Blob, from_cdrs::FromCDRSByName, rows::Row, IntoRustByName};
use cdrs::{query, query_values};
use std::io::Cursor;
use std::str::from_utf8;
use tokio::io::Error;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::mpsc;

app!(GetTrytesBuilder {
    listen_address: String
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            listen_address: self.listen_address.unwrap(),
            launcher_tx: self.launcher_tx,
        }
    }
}

#[derive(Debug, TryFromRow)]
pub struct TcpStreamTx {
    hash: Blob,
    payload: Blob,
    address: Blob,
    value: i32,
    obsolete_tag: Blob,
    timestamp: i32,
    current_index: i16,
    last_index: i16,
    bundle: Blob,
    trunk: Blob,
    branch: Blob,
    tag: Blob,
    attachment_timestamp: i32,
    attachment_timestamp_lower: i32,
    attachment_timestamp_upper: i32,
    nonce: Blob,
}

pub struct GetTrytes {
    listen_address: String,
    launcher_tx: Option<mpsc::UnboundedSender<String>>,
}

impl GetTrytes {
    // TODO: Error Handling
    pub async fn run(mut self, tx_hashs: Vec<Hash>) {
        for hash in tx_hashs.iter() {
            let mut stream = TcpStream::connect(self.listen_address.clone())
                .await
                .unwrap();
            self.get_trytes(hash, stream).await;
        }
    }
    pub async fn get_trytes(
        &self,
        tx_hash: &Hash,
        mut stream: TcpStream,
    ) -> Result<Transaction, Error> {
        // let mut cqlconn = connection?;
        // query param builder
        let params = query::QueryParamsBuilder::new()
            .values(query_values!(tx_hash.to_string()))
            .page_size(500)
            .finalize();
        let query = SELECT_TX_QUERY.to_string();
        let query = query::Query { query, params };
        // query_frame
        let query_frame = Frame::new_query(query, vec![Flag::Ignore]).into_cbytes();
        // write frame to stream
        // let mut stream = cqlconn.take_stream();
        stream.write(query_frame.as_slice()).await?;
        // read buffer
        let mut head_buffer = vec![0; 9];
        stream.read(&mut head_buffer).await?;
        let length = get_body_length_usize(&head_buffer);
        let mut body_buffer = vec![0; length];
        stream.read(&mut body_buffer).await?;
        let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
        let mut rows = frame_result::ResResultBody::from_cursor(&mut cursor)
            .unwrap()
            .into_rows()
            .unwrap();
        let tx = TcpStreamTx::try_from_row(rows.pop().unwrap()).unwrap();
        let mut builder = TransactionBuilder::new();
        builder = builder
            .with_payload(Payload::from_str(
                from_utf8(&tx.payload.into_vec()).unwrap(),
            ))
            .with_address(Address::from_str(
                from_utf8(&tx.address.into_vec()).unwrap(),
            ))
            .with_value(Value(tx.value as i64))
            .with_obsolete_tag(Tag::from_str(
                from_utf8(&tx.obsolete_tag.into_vec()).unwrap(),
            ))
            .with_timestamp(Timestamp(tx.timestamp as u64))
            .with_index(Index(tx.current_index as usize))
            .with_last_index(Index(tx.last_index as usize))
            .with_bundle(Hash::from_str(from_utf8(&tx.bundle.into_vec()).unwrap()))
            .with_trunk(Hash::from_str(from_utf8(&tx.trunk.into_vec()).unwrap()))
            .with_branch(Hash::from_str(from_utf8(&tx.branch.into_vec()).unwrap()))
            .with_tag(Tag::from_str(from_utf8(&tx.tag.into_vec()).unwrap()))
            .with_attachment_ts(Timestamp(tx.attachment_timestamp as u64))
            .with_attachment_lbts(Timestamp(tx.attachment_timestamp_lower as u64))
            .with_attachment_ubts(Timestamp(tx.attachment_timestamp_upper as u64))
            .with_nonce(Nonce::from_str(from_utf8(&tx.nonce.into_vec()).unwrap()));
        Ok(builder.build().unwrap())
    }
}
