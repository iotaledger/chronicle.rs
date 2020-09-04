use super::decoder::{
    bytes,
    Decoder,
    Frame,
};

pub struct AuthSuccess {
    token: Option<Vec<u8>>,
}

impl AuthSuccess {
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
}

impl From<&[u8]> for AuthSuccess {
    fn from(slice: &[u8]) -> Self {
        let token = bytes(slice);
        Self { token }
    }
}
