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
use scylla::access::insert::Insert;

macro_rules! impl_insert {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        #[async_trait]
        impl Insert<$key, $val> for $keyspace {
            async fn insert<T>(&self, worker: Box<T>, key: &$key, value: &$val)
            where
                T: scylla_cql::VoidDecoder<$key, $val> + scylla::Worker,
            $block
        }
    };
}

impl_insert!(Mainnet: <MessageId, Message> -> { todo!() });
impl_insert!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
impl_insert!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
impl_insert!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
impl_insert!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
impl_insert!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
impl_insert!(Mainnet: <Unspent, ()> -> { todo!() });
impl_insert!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
impl_insert!(Mainnet: <(), LedgerIndex> -> { todo!() });
impl_insert!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
impl_insert!(Mainnet: <(), SnapshotInfo> -> { todo!() });
impl_insert!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
impl_insert!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
impl_insert!(Mainnet: <Address, Balance> -> { todo!() });
impl_insert!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
