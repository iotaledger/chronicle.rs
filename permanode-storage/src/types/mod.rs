mod col_decode;

use std::ops::{
    Deref,
    DerefMut,
};

pub use bee_ledger::{
    balance::Balance,
    model::{
        OutputDiff,
        Unspent,
    },
};
pub use bee_message::{
    ledger_index::LedgerIndex,
    milestone::Milestone,
    prelude::{
        Address,
        ConsumedOutput,
        CreatedOutput,
        Ed25519Address,
        HashedIndex,
        MilestoneIndex,
        OutputId,
    },
    solid_entry_point::SolidEntryPoint,
    Message,
    MessageId,
};
pub use bee_snapshot::SnapshotInfo;
pub use bee_tangle::{
    metadata::MessageMetadata,
    unconfirmed_message::UnconfirmedMessage,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Bee<Type> {
    inner: Type,
}

impl<Type> Bee<Type> {
    pub fn wrap(t: Type) -> Bee<Type> {
        Bee { inner: t }
    }

    pub fn into_inner(self) -> Type {
        self.inner
    }
}

impl<Type> Deref for Bee<Type> {
    type Target = Type;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Type> DerefMut for Bee<Type> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Type> From<Type> for Bee<Type> {
    fn from(t: Type) -> Self {
        Bee::wrap(t)
    }
}
