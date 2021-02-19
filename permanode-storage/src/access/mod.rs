use crate::keyspaces::*;
use async_trait::async_trait;
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
pub use scylla::{
    access::{
        delete::*,
        insert::*,
        select::*,
        update::*,
    },
    stage::{
        ReporterEvent,
        ReporterHandle,
    },
    worker::WorkerError,
    Worker,
};
use scylla_cql::VoidDecoder;
pub use scylla_cql::{
    CqlError,
    Decoder,
    Frame,
    Query,
};

mod delete;
mod insert;
mod select;
mod update;

impl VoidDecoder for Mainnet {}
