use super::*;

macro_rules! impl_select {
    ($keyspace:ty: <$key:ty, $val:ty> -> $block:block) => {
        impl Select<$key, $val> for $keyspace {
            fn select(&self, key: &$key) -> SelectQuery<$key, $val>
            $block
        }
    };
}

macro_rules! impl_decode {
    ($keyspace:ty: <$val:ty> -> $block:block) => {
        impl RowsDecoder<$val> for $keyspace {
            fn try_decode(decoder: Decoder) -> Result<Option<$val>, CqlError>
            $block
        }
    };
}

impl_select!(Mainnet: <MessageId, Message> -> { todo!() });
impl_select!(Mainnet: <MessageId, MessageMetadata> -> { todo!() });
impl_decode!(Mainnet: <Message> -> { todo!()} );
impl_decode!(Mainnet: <MessageMetadata> -> { todo!()} );
// impl_select!(Mainnet: <(HashedIndex, MessageId), ()> -> { todo!() });
// impl_select!(Mainnet: <OutputId, CreatedOutput> -> { todo!() });
// impl_select!(Mainnet: <OutputId, ConsumedOutput> -> { todo!() });
// impl_select!(Mainnet: <Unspent, ()> -> { todo!() });
// impl_select!(Mainnet: <(Ed25519Address, OutputId), ()> -> { todo!() });
// impl_select!(Mainnet: <(), LedgerIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, Milestone> -> { todo!() });
// impl_select!(Mainnet: <(), SnapshotInfo> -> { todo!() });
// impl_select!(Mainnet: <SolidEntryPoint, MilestoneIndex> -> { todo!() });
// impl_select!(Mainnet: <MilestoneIndex, OutputDiff> -> { todo!() });
// impl_select!(Mainnet: <Address, Balance> -> { todo!() });
// impl_select!(Mainnet: <(MilestoneIndex, UnconfirmedMessage), ()> -> { todo!() });
