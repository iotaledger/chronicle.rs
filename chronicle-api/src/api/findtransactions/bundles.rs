use chronicle_cql::{
    frame::decoder::{
        Decoder,
        Frame,
    },
    rows,
};
rows!(
    rows: Hashes {},
    row: Row(
        Hash,
        Payload,
        Address,
        Value,
        ObsoleteTag,
        Timestamp,
        CurrentIndex,
        LastIndex,
        Bundle,
        Trunk,
        Branch,
        Tag,
        AttachmentTimestamp,
        AttachmentTimestampLower,
        AttachmentTimestampUpper,
        Nonce,
        Milestone
    ),
    column_decoder: BundlesDecoder
);

// define decoder trait as you wish
trait Rows {
    // to decode the rows
    fn decode(self) -> Self;
    // to finalize it as the expected result (trytes or none)
    fn finalize(self) -> Option<String>;
}

impl Rows for Hashes {
    fn decode(mut self) -> Self {
        // each next() call will decode one row
        self.next();
        // return
        self
    }
    fn finalize(self) -> Option<String> {
        // check if result was not empty
        if self.rows_count == 1 {
            // the buffer is ready to be converted to string trytes
            Some(String::from_utf8(self.decoder.into_buffer()).unwrap())
        } else {
            // we didn't have any transaction row for the provided hash.
            None
        }
    }
}
// implementation to decoder the columns in order to form the trytes eventually
impl BundlesDecoder for Hash {
    fn decode_column(start: usize, length: i32, acc: &mut Hashes) {
        acc.buffer().copy_within(start..(start + length as usize), 81)
    }
}
impl BundlesDecoder for Payload {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the payload to get the hashes, so nothing should be done.
    }
}
impl BundlesDecoder for Address {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the address to get the hashes, so nothing should be done.
    }
}
impl BundlesDecoder for Value {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the value to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for ObsoleteTag {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the value to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Timestamp {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the timestamp to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for CurrentIndex {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the currentindex to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for LastIndex {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the lastindex to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Bundle {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the bundle to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Trunk {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the trunk to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Branch {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the branch to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Tag {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the tag to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for AttachmentTimestamp {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the attachmenttimestamp to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for AttachmentTimestampLower {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the attachmenttimestamplower to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for AttachmentTimestampUpper {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the attachmenttimestampupper to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Nonce {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the nonce to get the hash, so nothing should be done.
    }
}
impl BundlesDecoder for Milestone {
    fn decode_column(_start: usize, _length: i32, _acc: &mut Hashes) {
        // we don't need the milestone to get the hash, so nothing should be done.
    }
}
