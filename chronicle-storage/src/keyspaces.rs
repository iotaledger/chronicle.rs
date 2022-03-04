// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use scylla_rs::prelude::Keyspace;
use serde::{
    Deserialize,
    Serialize,
};
use std::borrow::Cow;

/// The Chronicle keyspace
#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ChronicleKeyspace {
    name: Cow<'static, str>,
}

impl ChronicleKeyspace {
    /// Create a new instance of the keyspace
    pub fn new(name: String) -> Self {
        Self { name: name.into() }
    }
    /// Return the keyspace name as str
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for ChronicleKeyspace {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
