pub use scylla::access::Keyspace;
use std::borrow::Cow;

/// The Mainnet keyspace, which will organize its tables to pull data from the Mainnet tangle network
#[derive(Default)]
pub struct Mainnet {
    name: Cow<'static, str>,
}

impl Mainnet {
    /// Create a new instance of the Mainnet keyspace
    pub fn new() -> Self {
        Self { name: "mainnet".into() }
    }
}

impl Keyspace for Mainnet {
    fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
}
