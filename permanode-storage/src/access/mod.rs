use crate::keyspaces::Mainnet;
use bee_common::packable::Packable;
pub use rows::*;
pub use scylla::{
    access::*,
    stage::{
        ReporterEvent,
        ReporterHandle,
    },
    worker::WorkerError,
    Worker,
};
use scylla_cql::{
    ColumnDecoder,
    Metadata,
    Rows,
    RowsDecoder,
    TryInto,
    VoidDecoder,
};
pub use scylla_cql::{
    CqlError,
    Decoder,
    Execute,
    Frame,
    Query,
};
use std::marker::PhantomData;
pub use types::*;

mod insert;
#[allow(missing_docs)]
mod rows;
mod select;
mod types;

impl VoidDecoder for Mainnet {}

pub(crate) struct BeeRows<Type> {
    decoder: Decoder,
    pub rows_count: usize,
    pub remaining_rows_count: usize,
    _metadata: Metadata,
    column_start: usize,
    _type: PhantomData<Type>,
}

impl<Type> Iterator for BeeRows<Type>
where
    Bee<Type>: ColumnDecoder,
{
    type Item = Bee<Type>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows_count > 0 {
            self.remaining_rows_count -= 1;

            let length = i32::from_be_bytes(
                self.decoder.buffer_as_ref()[self.column_start..][..4]
                    .try_into()
                    .unwrap(),
            );
            self.column_start += 4; // now it become the column_value start, or next column_start if length < 0
            if length > 0 {
                let col_slice = self.decoder.buffer_as_ref()[self.column_start..][..(length as usize)].into();
                // update the next column_start to start from next column
                self.column_start += length as usize;
                Option::<Bee<Type>>::decode(col_slice)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<Type: Packable> Rows for BeeRows<Type> {
    fn new(decoder: Decoder) -> Self {
        let metadata = decoder.metadata();
        let rows_start = metadata.rows_start();
        let column_start = rows_start + 4;
        let rows_count = i32::from_be_bytes(decoder.buffer_as_ref()[rows_start..column_start].try_into().unwrap());
        Self {
            decoder,
            _metadata: metadata,
            rows_count: rows_count as usize,
            remaining_rows_count: rows_count as usize,
            column_start,
            _type: PhantomData,
        }
    }
}

impl<K, V> RowsDecoder<K, Bee<V>> for Mainnet
where
    V: Packable,
    Bee<V>: ColumnDecoder,
{
    fn try_decode(decoder: Decoder) -> Result<Option<Bee<V>>, CqlError> {
        if decoder.is_error() {
            Err(decoder.get_error())
        } else {
            let mut rows = BeeRows::<V>::new(decoder);
            Ok(rows.next())
        }
    }
}
