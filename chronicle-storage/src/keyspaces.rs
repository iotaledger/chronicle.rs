// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub use scylla_rs::prelude::Keyspace;
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
    fn name(&self) -> String {
        self.name.to_string()
    }
}

struct SomeType;

impl SomeType {
    fn what<
        S: 'static
            + scylla_rs::prelude::Select<
                String,
                chronicle_common::SyncRange,
                scylla_rs::prelude::Iter<crate::access::AnalyticRecord>,
            >,
    >() {
        println!("bound worked");
    }
}

#[test]
fn test_row_bound() {
    let keyspace = ChronicleKeyspace::new("test_keyspace".into());
    SomeType::what::<ChronicleKeyspace>();
    println!("{:?}", "outer");
}
