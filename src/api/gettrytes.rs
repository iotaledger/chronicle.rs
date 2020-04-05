use crate::connection::cql::get_body_length_usize;
use crate::ring::ring::{Ring, Token, DC};
use crate::stage::reporter;
use crate::statements::*;
use crate::worker::broker::{Broker, BrokerEvent, QueryRef};
use bee_bundle::{Hash, Transaction};
use bee_common::constants::TRANSACTION_TRIT_LEN;
use bee_ternary::{Trits, T5B1};
use cdrs::frame::traits::{FromCursor, TryFromRow};
use cdrs::frame::{frame_result, Flag, Frame, IntoBytes};
use cdrs::types::{blob::Blob, from_cdrs::FromCDRSByName, rows::Row};
use cdrs::{query, query_values};
use std::io::Cursor;
use tokio::sync::mpsc;

actor!(GetTrytesBuilder {
    _listen_address: String,
    data_center: DC,
    replica_index: usize,
    token: Token
});

impl GetTrytesBuilder {
    pub fn build(self) -> GetTrytes {
        GetTrytes {
            _listen_address: self._listen_address.unwrap(),
            data_center: self.data_center.unwrap(),
            replica_index: self.replica_index.unwrap(),
            token: self.token.unwrap(),
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
    attachment_ts: i32,
    attachment_lbts: i32,
    attachment_ubts: i32,
    nonce: Blob,
}

pub struct GetTrytes {
    _listen_address: String,
    data_center: DC,
    replica_index: usize,
    token: Token,
}

unsafe fn any_as_i8_slice<T: Sized>(p: &T) -> &[i8] {
    ::std::slice::from_raw_parts((p as *const T) as *const i8, ::std::mem::size_of::<T>())
}

impl GetTrytes {
    // TODO: Error Handling
    // TODO: refactory the directory struct
    // TODO: use Ring::send to send the queries
    // TODO: support to query multiple hashes at one run
    // TODO: global query ID
    pub async fn run(self, hash: &Hash) {
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
                    query: _,
                    tx: _,
                } => {
                    let head_buffer = &giveload[0..9];
                    let length = get_body_length_usize(&head_buffer);
                    let body_buffer = &giveload[9..length + 9];
                    let mut cursor: Cursor<&[u8]> = Cursor::new(&body_buffer);
                    let mut rows = frame_result::ResResultBody::from_cursor(&mut cursor)
                        .unwrap()
                        .into_rows()
                        .unwrap(); //TritBuf::from_trits(&tx.payload.into_vec()) //
                    let tx_struct = TcpStreamTx::try_from_row(rows.pop().unwrap()).unwrap();
                    let bytes: &[i8] = unsafe { any_as_i8_slice(&tx_struct) };
                    let _tx = Transaction::from_trits(unsafe {
                        Trits::<T5B1>::from_raw_unchecked(bytes, TRANSACTION_TRIT_LEN)
                    })
                    .unwrap();
                }
                _ => {}
            }
        }
    }
}
