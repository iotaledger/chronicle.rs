use crate::connection::cql::get_body_length_usize;

use crate::statements::*;
// use crate::worker::{Error, Status, StreamStatus, Worker};
use bee_bundle::{
    Address, Hash, Index, Nonce, Payload, Tag, Timestamp, TransactionBuilder, TransactionField,
    Value, ADDRESS_TRIT_LEN, HASH_TRIT_LEN, NONCE_TRIT_LEN, PAYLOAD_TRIT_LEN, TAG_TRIT_LEN,
};
use bytemuck::cast_slice;

use crate::ring::ring::{Ring, Token, DC};
use crate::stage::reporter;
use crate::worker::broker::{Broker, BrokerEvent, QueryRef};
use bee_ternary::{T1B1Buf, TritBuf, Trits, T5B1};
use cdrs::frame::traits::{FromCursor, TryFromRow};
use cdrs::frame::{frame_result, Flag, Frame, IntoBytes};
use cdrs::types::{blob::Blob, from_cdrs::FromCDRSByName, rows::Row};
use cdrs::{query, query_values};
use std::io::Cursor;
use tokio::sync::mpsc;

actor!(GetTrytesBuilder {
    listen_address: String,
    data_center: DC,
    replica_index: usize,
    token: Token
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            listen_address: self.listen_address.unwrap(),
            data_center: self.data_center.unwrap(),
            replica_index: self.replica_index.unwrap(),
            token: self.token.unwrap(),
        }
    }
}

// TODO: this function is currently borrowed from the storage crate, remove it in refactory phase
fn decode_bytes(u8_slice: &[u8], num_trits: usize) -> TritBuf {
    let decoded_column_i8_slice: &[i8] = cast_slice(u8_slice);
    unsafe {
        Trits::<T5B1>::from_raw_unchecked(decoded_column_i8_slice, num_trits).to_buf::<T1B1Buf>()
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
    attachment_ts: i32,
    attachment_lbts: i32,
    attachment_ubts: i32,
    nonce: Blob,
}

pub struct GetTrytes {
    listen_address: String,
    data_center: DC,
    replica_index: usize,
    token: Token,
}

impl GetTrytes {
    // TODO: Error Handling
    // TODO: refactory the directory struct
    // TODO: use Ring::send to send the queries
    // TODO: support to query multiple hashes at one run
    // TODO: global query ID
    pub async fn run(mut self, hash: &Hash) {
        // query param builder
        let params = query::QueryParamsBuilder::new()
            .values(query_values!(hash.as_bytes().to_vec()))
            .page_size(500)
            .finalize();
        let query = SELECT_TX_QUERY.to_string();
        let query = query::Query { query, params };
        // query_frame
        let query_frame = Frame::new_query(query, vec![Flag::Ignore]).into_cbytes();

        // TODO: modify the prepare_payload
        let qf = QueryRef::new(1, &[0; 200]);
        let (tx, mut rx) = mpsc::unbounded_channel::<BrokerEvent>();
        let worker = Broker::new(tx, qf);
        let event = reporter::Event::Request {
            worker: smallbox!(worker),
            payload: query_frame,
        };
        Ring::send(&self.data_center, self.replica_index, self.token, event);

        while let Some(event) = rx.recv().await {
            match event {
                BrokerEvent::Response {
                    giveload,
                    query,
                    tx,
                } => {
                    let head_buffer = &giveload[0..9];
                    let length = get_body_length_usize(&head_buffer);
                    let body_buffer = &giveload[9..length + 9];
                    let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
                    let mut rows = frame_result::ResResultBody::from_cursor(&mut cursor)
                        .unwrap()
                        .into_rows()
                        .unwrap();
                    let tx = TcpStreamTx::try_from_row(rows.pop().unwrap()).unwrap();
                    let payload_tritbuf = decode_bytes(&tx.payload.into_vec(), PAYLOAD_TRIT_LEN);
                    let address_tritbuf = decode_bytes(&tx.address.into_vec(), ADDRESS_TRIT_LEN);
                    let value = tx.value as i64;
                    let obs_tag_tritbuf = decode_bytes(&tx.obsolete_tag.into_vec(), TAG_TRIT_LEN);
                    let timestamp = tx.timestamp as u64;
                    let current_index = tx.current_index as usize;
                    let last_index = tx.last_index as usize;
                    let bundle_tritbuf = decode_bytes(&tx.bundle.into_vec(), HASH_TRIT_LEN);
                    let tag_tritbuf = decode_bytes(&tx.tag.into_vec(), TAG_TRIT_LEN);
                    let trunk_tritbuf = decode_bytes(&tx.trunk.into_vec(), TAG_TRIT_LEN);
                    let branch_tritbuf = decode_bytes(&tx.branch.into_vec(), TAG_TRIT_LEN);
                    let attachment_ts = tx.attachment_ts as u64;
                    let attachment_lbts = tx.attachment_lbts as u64;
                    let attachment_ubts = tx.attachment_ubts as u64;
                    let nonce_tritbuf = decode_bytes(&tx.nonce.into_vec(), NONCE_TRIT_LEN);
                    let builder = TransactionBuilder::new()
                        .with_payload(Payload::from_inner_unchecked(payload_tritbuf))
                        .with_address(Address::from_inner_unchecked(address_tritbuf))
                        .with_value(Value::from_inner_unchecked(value))
                        .with_obsolete_tag(Tag::from_inner_unchecked(obs_tag_tritbuf))
                        .with_timestamp(Timestamp::from_inner_unchecked(timestamp))
                        .with_index(Index::from_inner_unchecked(current_index))
                        .with_last_index(Index::from_inner_unchecked(last_index))
                        .with_bundle(Hash::from_inner_unchecked(bundle_tritbuf))
                        .with_trunk(Hash::from_inner_unchecked(trunk_tritbuf))
                        .with_branch(Hash::from_inner_unchecked(branch_tritbuf))
                        .with_tag(Tag::from_inner_unchecked(tag_tritbuf))
                        .with_attachment_ts(Timestamp::from_inner_unchecked(attachment_ts))
                        .with_attachment_lbts(Timestamp::from_inner_unchecked(attachment_lbts))
                        .with_attachment_ubts(Timestamp::from_inner_unchecked(attachment_ubts))
                        .with_nonce(Nonce::from_inner_unchecked(nonce_tritbuf));
                    // TODO: return the decoded transaction
                    let tx = builder.build();
                }
                BrokerEvent::StreamStatus {
                    stream_status,
                    query,
                    tx,
                } => {}
                BrokerEvent::Error { kind, query, tx } => {}
            }
        }
    }
}
