use super::decoder::{bytes, Decoder, Frame};

#[derive(Debug)]
pub struct AuthChallenge {
    token: Option<Vec<u8>>,
}

impl AuthChallenge {
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
}

impl From<&[u8]> for AuthChallenge {
    fn from(slice: &[u8]) -> Self {
        let token = bytes(slice);
        Self { token }
    }
}
