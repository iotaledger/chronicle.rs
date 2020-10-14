use super::decoder::{string, Decoder, Frame};

pub struct Authenticate {
    pub authenticator: String,
}

impl Authenticate {
    pub fn new(decoder: &Decoder) -> Self {
        Self::from(decoder.body())
    }
    pub fn authenticator(&self) -> &str {
        &self.authenticator[..]
    }
}

impl From<&[u8]> for Authenticate {
    fn from(slice: &[u8]) -> Self {
        let authenticator = string(slice);
        Self { authenticator }
    }
}
