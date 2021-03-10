pub use crate::keyspaces::PermanodeKeyspace;
use bee_common::packable::Packable;
pub use scylla::access::*;
use scylla_cql::{
    ColumnDecoder,
    ColumnEncoder,
    PreparedStatement,
    RowsDecoder,
    VoidDecoder,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    io::Cursor,
    ops::Deref,
};

use bincode::Options;
pub use types::*;

mod insert;
mod select;
mod token;
mod types;

impl VoidDecoder for PermanodeKeyspace {}

use bincode::config::*;
#[allow(unused)]
pub(crate) type BincodeOptions =
    WithOtherTrailing<WithOtherIntEncoding<WithOtherEndian<DefaultOptions, BigEndian>, FixintEncoding>, AllowTrailing>;
#[allow(unused)]
pub(crate) fn bincode_config() -> BincodeOptions {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

pub struct Row<T> {
    inner: T,
}

impl<T> Deref for Row<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Row<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn rows_iter(decoder: Decoder) -> scylla::access::Iter<Self>
    where
        Self: scylla::access::Row,
    {
        scylla::access::Iter::<Self>::new(decoder)
    }
}
