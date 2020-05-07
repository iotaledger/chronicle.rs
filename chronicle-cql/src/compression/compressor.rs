pub trait Compressor {
    fn compress(&self, buffer: &mut Vec<u8>);
}
