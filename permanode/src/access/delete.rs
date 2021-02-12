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
use scylla::access::delete::Delete;

macro_rules! impl_delete {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        #[async_trait]
        impl Delete<$key, $val> for $keyspace {
            async fn delete<T>(&self, worker: T, key: &$key)
            where
                T: scylla_cql::VoidDecoder<$key, $val> + scylla::Worker,
            $block
        }
    };
}

impl_delete!(Mainnet: <MessageId, Message> -> { todo!() });
impl_delete!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
impl_delete!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
impl_delete!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
impl_delete!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
impl_delete!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
impl_delete!(Mainnet: <Unspent, ()> -> { todo!() });
impl_delete!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
impl_delete!(Mainnet: <(), LedgerIndex> -> { todo!() });
impl_delete!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
impl_delete!(Mainnet: <(), SnapshotInfo> -> { todo!() });
impl_delete!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
impl_delete!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
impl_delete!(Mainnet: <Address, Balance> -> { todo!() });
impl_delete!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
