// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use scylla::access::Keyspace;
use std::borrow::Cow;

/// The Chronicle keyspace
#[derive(Default, Clone, Debug)]
pub struct ChronicleKeyspace {
    name: Cow<'static, str>,
}

impl ChronicleKeyspace {
    /// Create a new instance of the keyspace
    pub fn new(name: String) -> Self {
        Self { name: name.into() }
    }
}

impl Keyspace for ChronicleKeyspace {
    fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
}
