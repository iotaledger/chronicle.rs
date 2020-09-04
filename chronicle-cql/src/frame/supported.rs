use super::decoder::{
    string_multimap,
    Decoder,
    Frame,
};
use std::collections::HashMap;

pub struct Supported {
    options: HashMap<String, Vec<String>>,
}

impl Supported {
    pub fn new(decoder: &Decoder) -> Self {
        let options = string_multimap(decoder.body());
        Self { options }
    }
    pub fn get_options(&self) -> &HashMap<String, Vec<String>> {
        &self.options
    }
}
