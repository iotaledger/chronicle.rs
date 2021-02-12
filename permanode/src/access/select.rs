use super::*;
use bee_ledger::{
    balance::Balance,
    model::{
        OutputDiff,
        Unspent,
    },
};
use bee_message::{
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
use bee_snapshot::SnapshotInfo;
use bee_tangle::{
    metadata::MessageMetadata,
    unconfirmed_message::UnconfirmedMessage,
};
use scylla::access::select::Select;

macro_rules! impl_select {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        #[async_trait]
        impl Select<$key, $val> for $keyspace {
            async fn select<T>(&self, worker: Box<T>, key: &$key)
            where
                T: scylla_cql::RowsDecoder<$key, $val> + scylla::Worker,
            $block
        }
    };
}

impl_select!(Mainnet: <MessageId, Message> -> { todo!() });
impl_select!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
impl_select!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
impl_select!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
impl_select!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
impl_select!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
impl_select!(Mainnet: <Unspent, ()> -> { todo!() });
impl_select!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
impl_select!(Mainnet: <(), LedgerIndex> -> { todo!() });
impl_select!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
impl_select!(Mainnet: <(), SnapshotInfo> -> { todo!() });
impl_select!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
impl_select!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
impl_select!(Mainnet: <Address, Balance> -> { todo!() });
impl_select!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
