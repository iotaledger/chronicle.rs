pub use crate::keyspaces::Mainnet;
use bee_common::packable::Packable;
pub use rows::*;
pub use scylla::access::*;
use scylla_cql::{
    ColumnDecoder,
    ColumnEncoder,
    Metadata,
    PreparedStatement,
    QueryStatement,
    Rows,
    RowsDecoder,
    TryInto,
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
use std::marker::PhantomData;
pub use types::*;

mod insert;
#[allow(missing_docs)]
mod rows;
mod select;
mod token;
mod types;
impl VoidDecoder for Mainnet {}

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

impl<T> Row<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
}
