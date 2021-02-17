use super::*;

macro_rules! impl_insert {
    ($keyspace:ty: <$val:ty> -> $block:block) => {
        impl Insert<$val> for $keyspace {
            fn insert(&self, value: &$val) -> InsertQuery<$val>
            $block
        }
    };
}

impl_insert!(Mainnet: <Message> -> { todo!() });
// impl_insert!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
// impl_insert!(Mainnet: <(MessageId, MessageId), ()> -> { todo!() });
// impl_insert!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_insert!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_insert!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_insert!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_insert!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_insert!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_insert!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
// impl_insert!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_insert!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_insert!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_insert!(Mainnet: <Address, Balance> -> { todo!() });
// impl_insert!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
