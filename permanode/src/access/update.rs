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
use scylla::access::update::Update;

macro_rules! impl_update {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        #[async_trait]
        impl Update<$key, $val> for $keyspace {
            async fn update<T>(&self, worker: Box<T>, key: &$key, value: &$val)
            where
                T: scylla_cql::VoidDecoder<$key, $val> + scylla::Worker,
            $block
        }
    };
}

impl_update!(Mainnet: <MessageId, Message> -> { todo!() });
impl_update!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
impl_update!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
impl_update!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
impl_update!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
impl_update!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
impl_update!(Mainnet: <Unspent, ()> -> { todo!() });
impl_update!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
impl_update!(Mainnet: <(), LedgerIndex> -> { todo!() });
impl_update!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
impl_update!(Mainnet: <(), SnapshotInfo> -> { todo!() });
impl_update!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
impl_update!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
impl_update!(Mainnet: <Address, Balance> -> { todo!() });
impl_update!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
