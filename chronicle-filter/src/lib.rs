// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use bee_message::{
    Message,
    MessageId,
};
use chronicle_storage::access::MessageMetadata;
use serde::{
    Deserialize,
    Serialize,
};

use std::fmt::Debug;
use wildmatch::WildMatch;

#[derive(Debug, Copy, Clone)]
pub struct Selected {
    /// Store proof in the database
    require_proof: bool,
}
impl Selected {
    pub fn select() -> Self {
        Self { require_proof: false }
    }
    pub fn with_proof(mut self) -> Self {
        self.require_proof = true;
        self
    }
    /// Check if we have to store the proof of inclusion
    pub fn require_proof(&self) -> bool {
        self.require_proof
    }
}

#[async_trait::async_trait]
pub trait SelectiveBuilder: 'static + Debug + PartialEq + Eq + Sized + Send + Clone + Serialize + Sync {
    type State: Selective;
    async fn build(self) -> anyhow::Result<Self::State>;
}

#[async_trait::async_trait]
pub trait Selective: Clone + Sized + Send {
    /// Define if the trait is being implemented on permanode(true) or selective-permanode(false)
    const PERMANODE: bool;
    /// Check if this is running in permanode mode.
    fn is_permanode(&self) -> bool {
        Self::PERMANODE
    }
    /// invoked when you
    async fn filter_message(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        metadata: Option<&MessageMetadata>,
    ) -> anyhow::Result<Option<Selected>>;
    // todo add async fn selected_messages()
}

// todo move these to the root bin crate
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermanodeConfig;
#[derive(Clone, Debug)]
pub struct Permanode;

#[async_trait::async_trait]
impl SelectiveBuilder for PermanodeConfig {
    type State = Permanode;
    async fn build(self) -> anyhow::Result<Self::State> {
        Ok(Permanode)
    }
}

#[async_trait::async_trait]
impl Selective for Permanode {
    const PERMANODE: bool = true;
    #[inline]
    async fn filter_message(
        &mut self,
        message_id: &MessageId,
        message: &Message,
        metadata: Option<&MessageMetadata>,
    ) -> anyhow::Result<Option<Selected>> {
        Ok(Some(Selected::select()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectivePermanodeConfig {
    indexation_keys: Vec<u8>,
}

// #[async_trait::async_trait]
// impl SelectiveBuilder for SelectivePermanodeConfig {
// type State = ();
// async fn build(self) -> Result<(), Box<dyn std::error::Error>> {
// Ok(())
// }
// }

// todo impl deserialize and serialize
pub enum IndexationKey {
    Text(WildMatch),
    Hex(WildMatch),
}

struct HexedIndex {
    /// The hexed index with wildcard
    hexed_index: WildMatch,
}

impl HexedIndex {
    fn new(hexed_index: WildMatch) -> Self {
        Self { hexed_index }
    }
}
