use super::*;
use bee_common::packable::Packable;
use scylla_cql::ColumnDecoder;
use std::io::Cursor;

impl<P: Packable> ColumnDecoder for Bee<P> {
    fn decode(slice: &[u8]) -> Self {
        P::unpack(&mut Cursor::new(slice)).unwrap().into()
    }
}
