use super::*;
use bee_message::{
    address::Address,
    output::{
        feature_block::{
            IssuerFeatureBlock,
            MetadataFeatureBlock,
            SenderFeatureBlock,
            TagFeatureBlock,
        },
        AliasId,
        FoundryId,
        NativeTokens,
        NftId,
        OutputId,
        TokenScheme,
        UnlockConditions,
    },
};

use primitive_types::U256;

use bee_tangle::ConflictReason;
use chronicle_common::Wrapper;
use chrono::{
    offset::Utc,
    DateTime,
};
use futures::{
    stream::Stream,
    task::{
        Context,
        Poll,
    },
};
use pin_project_lite::pin_project;
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
        VecDeque,
    },
    io::Cursor,
    ops::{
        Deref,
        DerefMut,
    },
    path::PathBuf,
    str::FromStr,
};
/// Index type
pub type Index = u16;
/// Amount type
pub type Amount = u64;
/// Output type
pub type OutputType = u8;
/// ParentIndex type
pub type ParentIndex = u16;
/// Identify theoretical nodeid which updated/set the synced_by column in sync table
pub type SyncedBy = u8;
/// Identify theoretical nodeid which updated/set the logged_by column in sync table.
/// This enables the admin to locate the generated logs across cluster of chronicles
pub type LoggedBy = u8;
/// Reflect any type version
pub type Version = u8;
/// Milestone Range Id
pub type MsRangeId = u32;

/// A marker for a paged result
#[derive(Clone, Debug)]
pub struct Paged<T> {
    inner: T,
    /// The paging state for the query
    pub paging_state: Option<Vec<u8>>,
}

impl<T> Paged<T> {
    /// Creates a new paged marker with an inner type and a paging state
    pub fn new(inner: T, paging_state: Option<Vec<u8>>) -> Self {
        Self { inner, paging_state }
    }
}

impl<T> Deref for Paged<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Paged<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Wrapper for json data
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct JsonData<T> {
    data: T,
}

impl<T> Deref for JsonData<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> Wrapper for JsonData<T> {
    fn into_inner(self) -> Self::Target {
        self.data
    }
}

#[derive(Clone, Debug)]
/// Wrapper around MessageCount u32
pub struct MessageCount(pub u32);
impl Deref for MessageCount {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Clone, Debug)]
/// Wrapper around TransactionCount u32
pub struct TransactionCount(pub u32);
impl Deref for TransactionCount {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Clone, Debug)]
/// Wrapper around MessageCount u64
pub struct TransferredTokens(pub u64);
impl Deref for TransferredTokens {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A 'sync' table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct SyncRecord {
    pub milestone_index: MilestoneIndex,
    pub synced_by: Option<SyncedBy>,
    pub logged_by: Option<LoggedBy>,
}

impl SyncRecord {
    /// Creates a new sync row
    pub fn new(milestone_index: MilestoneIndex, synced_by: Option<SyncedBy>, logged_by: Option<LoggedBy>) -> Self {
        Self {
            milestone_index,
            synced_by,
            logged_by,
        }
    }
}

/// MessageMetadata storage object
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMetadata {
    #[serde(rename = "messageId")]
    pub message_id: MessageId,
    #[serde(rename = "parentMessageIds")]
    pub parent_message_ids: Vec<MessageId>,
    #[serde(rename = "isSolid")]
    pub is_solid: bool,
    #[serde(rename = "referencedByMilestoneIndex")]
    pub referenced_by_milestone_index: Option<u32>,
    #[serde(rename = "ledgerInclusionState")]
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
    #[serde(rename = "ConflictReason")]
    pub conflict_reason: Option<ConflictReason>,
    #[serde(rename = "shouldPromote")]
    pub should_promote: Option<bool>,
    #[serde(rename = "shouldReattach")]
    pub should_reattach: Option<bool>,
}

/// A message's ledger inclusion state
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LedgerInclusionState {
    /// A conflicting message, ex. a double spend
    #[serde(rename = "conflicting")]
    Conflicting = 0,
    /// A successful, included message
    #[serde(rename = "included")]
    Included = 1,
    /// A message without a transaction
    #[serde(rename = "noTransaction")]
    NoTransaction = 2,
}

impl TryFrom<u8> for LedgerInclusionState {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Conflicting),
            1 => Ok(Self::Included),
            2 => Ok(Self::NoTransaction),
            n => bail!("Unexpected ledger inclusion byte state: {}", n),
        }
    }
}

impl ColumnEncoder for LedgerInclusionState {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let num = unsafe { std::mem::transmute::<&LedgerInclusionState, &u8>(self) };
        num.encode(buffer)
    }
}

impl ColumnDecoder for LedgerInclusionState {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        u8::try_decode_column(slice)?.try_into()
    }
}

/// A `bee` type wrapper which is used to apply the `ColumnEncoder`
/// functionality over predefined types which are `Packable`.
#[derive(Copy, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub struct Bee<Type>(pub Type);

impl<Type> Bee<Type> {
    /// Consume the wrapper and return the inner `bee` type
    pub fn into_inner(self) -> Type {
        self.0
    }
}

impl<Type> Bee<Type> {
    pub fn as_ref(&self) -> Bee<&Type> {
        Bee(&self.0)
    }

    pub fn as_mut(&mut self) -> Bee<&mut Type> {
        Bee(&mut self.0)
    }
}

impl<Type> Deref for Bee<Type> {
    type Target = Type;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Type> DerefMut for Bee<Type> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<Type> From<Type> for Bee<Type> {
    fn from(t: Type) -> Self {
        Bee(t)
    }
}

impl TokenEncoder for Bee<OutputId> {
    fn encode_token(&self) -> TokenEncodeChain {
        Bee(self.transaction_id()).chain(&self.index())
    }
}

impl TokenEncoder for Bee<Milestone> {
    fn encode_token(&self) -> TokenEncodeChain {
        Bee(self.message_id()).chain(&self.timestamp())
    }
}
impl TokenEncoder for Hint {
    fn encode_token(&self) -> TokenEncodeChain {
        self.hint.encode_token()
    }
}
macro_rules! impl_simple_packable {
    ($t:ty) => {
        impl ColumnDecoder for Bee<$t> {
            fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                <$t>::unpack(&mut Cursor::new(slice))
                    .map_err(|e| anyhow!("{:?}", e))
                    .map(Into::into)
            }
        }

        impl ColumnEncoder for Bee<$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<&$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&mut $t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.pack(buffer).ok();
            }
        }

        impl TokenEncoder for Bee<&mut $t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }
    };
}

macro_rules! impl_string_packable {
    ($t:ty) => {
        impl ColumnDecoder for Bee<$t> {
            fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
                <$t>::from_str(&String::try_decode_column(slice)?)
                    .map_err(|e| anyhow!("{:?}", e))
                    .map(Into::into)
            }
        }

        impl ColumnEncoder for Bee<$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&$t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<&$t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }

        impl ColumnEncoder for Bee<&mut $t> {
            fn encode(&self, buffer: &mut Vec<u8>) {
                self.to_string().encode(buffer)
            }
        }

        impl TokenEncoder for Bee<&mut $t> {
            fn encode_token(&self) -> TokenEncodeChain {
                self.into()
            }
        }
    };
}

impl ColumnDecoder for Bee<MilestoneIndex> {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        let ms_index = u32::try_decode_column(slice)?;
        Ok(Bee(MilestoneIndex(ms_index)))
    }
}

impl ColumnEncoder for Bee<MilestoneIndex> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0 .0.encode(buffer)
    }
}

impl TokenEncoder for Bee<MilestoneIndex> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl ColumnEncoder for Bee<&MilestoneIndex> {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.0 .0.encode(buffer)
    }
}

impl_simple_packable!(Message);
impl_string_packable!(MessageId);
impl_string_packable!(OutputId);
impl_string_packable!(TransactionId);
impl_string_packable!(Ed25519Address);

/// Transaction variants. Can be Input, Output, or Unlock.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum TransactionVariant {
    /// A transaction's Input, which spends a prior Output
    Input = 0,
    /// A transaction's Unspent Transaction Output (UTXO), specifying an address to receive the funds
    Output = 1,
    /// A transaction's Unlock Block, used to unlock an Input for verification
    Unlock = 2,
    /// A Treasury transaction, used to point ot treasury payload
    Treasury = 3,
}

impl ColumnDecoder for TransactionVariant {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(match std::str::from_utf8(slice)? {
            "input" => TransactionVariant::Input,
            "output" => TransactionVariant::Output,
            "unlock" => TransactionVariant::Unlock,
            "treasury" => TransactionVariant::Treasury,
            _ => bail!("Unexpected variant type"),
        })
    }
}

impl ColumnEncoder for TransactionVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        let variant;
        match self {
            TransactionVariant::Input => variant = "input",
            TransactionVariant::Output => variant = "output",
            TransactionVariant::Unlock => variant = "unlock",
            TransactionVariant::Treasury => variant = "treasury",
        }
        buffer.extend(&i32::to_be_bytes(variant.len() as i32));
        buffer.extend(variant.as_bytes());
    }
}

/// Chronicle Message record
pub struct MessageRecord {
    pub message_id: MessageId,
    pub message: Message,
    pub version: u8,
    pub milestone_index: Option<MilestoneIndex>,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub conflict_reason: Option<ConflictReason>,
    pub proof: Option<Vec<u8>>,
}

impl MessageRecord {
    /// Return Message id of the message
    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }
    /// Return the message
    pub fn message(&self) -> &Message {
        &self.message
    }
    /// Return conflict_reason
    pub fn version(&self) -> u8 {
        self.version
    }
    /// Return referenced milestone index
    pub fn milestone_index(&self) -> Option<&MilestoneIndex> {
        self.milestone_index.as_ref()
    }
    /// Return inclusion_state
    pub fn inclusion_state(&self) -> Option<&LedgerInclusionState> {
        self.inclusion_state.as_ref()
    }
    /// Return conflict_reason
    pub fn conflict_reason(&self) -> Option<&ConflictReason> {
        self.conflict_reason.as_ref()
    }
    /// Return proof
    pub fn proof(&self) -> Option<&Proof> {
        self.proof.as_ref()
    }
}

/// A `parents` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct ParentRecord {
    pub milestone_index: Option<MilestoneIndex>,
    pub ms_timestamp: Option<DateTime<Utc>>,
    pub message_id: MessageId,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    /// Creates a new parent row
    pub fn new(
        milestone_index: Option<MilestoneIndex>,
        ms_timestamp: Option<DateTime<Utc>>,
        message_id: MessageId,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_timestamp,
            message_id,
            inclusion_state,
        }
    }
}

/// A tag hint, used to lookup in the `tags hints` table
#[derive(Clone, Debug)]
pub struct TagHint {
    /// The tag string
    pub tag: String,
    /// The tag hint variant. Can be 'Regular', 'ExtOutput', or 'NftOutput'.
    pub variant: TagHintVariant,
}

impl TagHint {
    /// Creates a new tagged or indexation hint
    pub fn regular(tag: String) -> Self {
        Self {
            tag,
            variant: TagHintVariant::Regular,
        }
    }
    /// Creates a new tag hint derived from feature block inside extended output
    pub fn extended_output(tag: String) -> Self {
        Self {
            tag,
            variant: TagHintVariant::ExtOutput,
        }
    }
    /// Creates a new tag hint derived from feature block inside nft output
    pub fn extended_output(tag: String) -> Self {
        Self {
            tag,
            variant: TagHintVariant::NftOutput,
        }
    }
    /// Get the tag string
    pub fn tag(&self) -> &String {
        &self.tag
    }
    /// Get the tag hint variant
    fn variant(&self) -> &TagHintVariant {
        &self.variant
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum TagHintVariant {
    /// An unhashed indexation key or tagged data
    Regular,
    /// A feature block for extended output
    ExtOutput,
    /// A feature block for nft output
    NftOutput,
}

impl std::fmt::Display for HintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                HintVariant::Regular => "Regular",
                HintVariant::ExtOutput => "ExtOutput",
                HintVariant::NftOutput => "NftOutput",
            }
        )
    }
}

impl ColumnEncoder for TagHintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}

///////////
/// Address hint, used to lookup in the `addresses hints` table
#[derive(Clone, Debug)]
pub struct AddressHint {
    /// The address
    pub address: Address,
    pub output_kind: OutputVariant,
    /// The tag hint variant. Can be 'Regular', 'ExtOutput', or 'NftOutput'.
    pub variant: AddressHintVariant,
}

impl AddressHint {
    /// Creates address hint
    pub fn new(address: Address, output_kind: OutputVariant, variant: AddressHintVariant) -> Self {
        Self {
            address,
            output_kind,
            variant,
        }
    }
    /// Get the address
    pub fn address(&self) -> &Address {
        &self.address
    }
    /// Get the output kind
    fn kind(&self) -> &OutputVariant {
        &self.output_kind
    }
    /// Get the address variant
    fn variant(&self) -> &AddressHintVariant {
        &self.variant
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum AddressHintVariant {
    Address,
    Sender,
    Issuer,
    StateController,
    Governor,
}

impl std::fmt::Display for AddressHintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Address => "Address",
                Self::Sender => "Sender",
                Self::Issuer => "Issuer",
                Self::StateController => "StateController",
                Self::Governor => "Governor",
            }
        )
    }
}

impl ColumnEncoder for AddressHintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}

#[derive(Clone, Debug)]
pub enum OutputVariant {
    Legacy,
    Extended,
    Alias,
    Foundry,
    Nft,
}

impl std::fmt::Display for OutputVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Legacy => "Legacy",
                Self::Extended => "Extended",
                Self::Alias => "Alias",
                Self::Nft => "Nft",
                Self::Foundry => "Foundry",
            }
        )
    }
}

impl ColumnEncoder for OutputVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
    }
}

/// A transaction's unlock data, to be stored in a `transactions` row.
/// Holds a reference to the input which it signs.
#[derive(Debug, Clone)]
pub struct UnlockData {
    /// it holds the transaction_id of the input which created the unlock_block
    pub input_tx_id: TransactionId,
    /// it holds the input_index of the input which created the unlock_block
    pub input_index: u16,
    /// it's the unlock_block
    pub unlock_block: UnlockBlock,
}
impl UnlockData {
    /// Creates a new unlock data
    pub fn new(input_tx_id: TransactionId, input_index: u16, unlock_block: UnlockBlock) -> Self {
        Self {
            input_tx_id,
            input_index,
            unlock_block,
        }
    }
}

impl Packable for UnlockData {
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        self.input_tx_id.pack(packer)?;
        self.input_index.pack(packer)?;
        self.unlock_block.pack(packer)?;
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, packable::error::UnpackError<Self::UnpackError, U::Error>> {
        Ok(Self {
            input_tx_id: TransactionId::unpack(unpacker)?,
            input_index: u16::unpack(unpacker)?,
            unlock_block: UnlockBlock::unpack(unpacker)?,
        })
    }
}

/// A transaction's input data, to be stored in a `transactions` row.
#[derive(Debug, Clone)]
pub enum InputData {
    /// An regular Input which spends a prior Output and its unlock block
    Utxo(UtxoInput, UnlockBlock),
    /// A special input for migrating funds from another network
    Treasury(TreasuryInput),
}

impl InputData {
    /// Creates a regular Input Data
    pub fn utxo(utxo_input: UtxoInput, unlock_block: UnlockBlock) -> Self {
        Self::Utxo(utxo_input, unlock_block)
    }
    /// Creates a special migration Input Data
    pub fn treasury(treasury_input: TreasuryInput) -> Self {
        Self::Treasury(treasury_input)
    }
}

impl Packable for InputData {
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        match self {
            InputData::Utxo(utxo_input, unlock_block) => {
                0u8.pack(packer)?;
                utxo_input.pack(packer)?;
                unlock_block.pack(packer)?;
            }
            InputData::Treasury(treasury_input) => {
                1u8.pack(packer)?;
                treasury_input.pack(packer)?;
            }
        }
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, packable::error::UnpackError<Self::UnpackError, U::Error>> {
        Ok(match u8::unpack(unpacker)? {
            0 => InputData::Utxo(UtxoInput::unpack(unpacker)?, UnlockBlock::unpack(unpacker)?),
            1 => InputData::Treasury(TreasuryInput::unpack(unpacker)?),
            _ => anyhow::bail!("Tried to unpack an invalid inputdata variant!"),
        })
    }
}

#[derive(Debug, Clone)]
/// Chrysalis transaction data
pub enum TransactionData {
    /// An unspent transaction input
    Input(InputData),
    /// A transaction output
    Output(Output),
    /// A signed block which can be used to unlock an input
    Unlock(UnlockData),
}

impl Packable for TransactionData {
    type UnpackError = anyhow::Error;
    fn pack<P: Packer>(&self, packer: &mut P) -> Result<(), P::Error> {
        match self {
            TransactionData::Input(input_data) => {
                0u8.pack(packer)?;
                input_data.pack(packer)?;
            }
            TransactionData::Output(output) => {
                1u8.pack(packer)?;
                output.pack(packer)?;
            }
            TransactionData::Unlock(block_data) => {
                2u8.pack(packer)?;
                block_data.pack(packer)?;
            }
        }
        Ok(())
    }
    fn unpack<U: Unpacker, const VERIFY: bool>(
        unpacker: &mut U,
    ) -> Result<Self, packable::error::UnpackError<Self::UnpackError, U::Error>> {
        Ok(match u8::unpack(unpacker)? {
            0 => TransactionData::Input(InputData::unpack(unpacker)?),
            1 => TransactionData::Output(Output::unpack(unpacker)?),
            2 => TransactionData::Unlock(UnlockData::unpack(unpacker)?),
            n => anyhow::bail!("Tried to unpack an invalid transaction variant!, tag: {}", n),
        })
    }
}

impl ColumnDecoder for TransactionData {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Self::unpack(&mut Cursor::new(slice)).map(Into::into)
    }
}

/// A `transactions` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct TransactionRecord {
    pub idx: Index,
    pub variant: TransactionVariant,
    pub message_id: MessageId,
    pub version: Version,
    pub data: TransactionData,
    pub inclusion_state: Option<LedgerInclusionState>,
    pub milestone_index: Option<MilestoneIndex>,
}

impl TransactionRecord {
    /// Creates an input transactions record
    pub fn input(
        idx: Index,
        message_id: MessageId,
        input_data: InputData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            idx,
            variant: TransactionVariant::Input,
            message_id,
            version: 1,
            data: TransactionData::Input(input_data),
            inclusion_state,
            milestone_index,
        }
    }
    /// Creates an output transactions record
    pub fn output(
        idx: Index,
        message_id: MessageId,
        data: Output,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            idx,
            variant: TransactionVariant::Output,
            message_id,
            version: 1,
            data: TransactionData::Output(data),
            inclusion_state,
            milestone_index,
        }
    }
    /// Creates an unlock block transactions record
    pub fn unlock(
        idx: Index,
        message_id: MessageId,
        data: UnlockData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            idx,
            variant: TransactionVariant::Unlock,
            message_id,
            version: 1,
            data: TransactionData::Unlock(data),
            inclusion_state,
            milestone_index,
        }
    }
}

/// A `Legacy Output` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct LegacyOutputRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_range_id: u32,
    pub output_type: OutputType,
    pub address: Address,
    pub amount: Amount,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl LegacyOutputRecord {
    /// Creates a new legacy output row row
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_timestamp: DateTime<Utc>,
        ms_range_id: u32,
        output_type: u8,
        address: Address,
        amount: Amount,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            output_type,
            address,
            amount,
            inclusion_state,
        }
    }
}

/// A `Basic Output` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct BasicOutputRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_range_id: u32,
    pub address: Address,
    pub amount: Amount,
    pub native_tokens: NativeTokens,
    pub unlock_conditions: UnlockConditions,
    pub sender: Option<SenderFeatureBlock>,
    pub tag: Option<TagFeatureBlock>,
    pub metadata: Option<MetadataFeatureBlock>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl BasicOutputRecord {
    /// Creates a new basic output row
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_timestamp: DateTime<Utc>,
        ms_range_id: u32,
        address: Address,
        amount: Amount,
        native_tokens: NativeTokens,
        unlock_conditions: UnlockConditions,
        tag: Option<TagFeatureBlock>,
        sender: Option<SenderFeatureBlock>,
        metadata: Option<MetadataFeatureBlock>,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            address,
            amount,
            tag,
            native_tokens,
            metadata,
            unlock_conditions,
            inclusion_state,
            sender,
        }
    }
}

/// An `Alias Output` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct AliasOutputRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_range_id: u32,
    pub ms_timestamp: DateTime<Utc>,
    pub amount: Amount,
    pub issuer: Option<Address>,
    pub state_controller: Option<Address>,
    pub native_tokens: NativeTokens,
    pub governor: Option<Address>,
    pub metadata: Option<MetadataFeatureBlock>,
    pub state_index: u32,
    pub state_metadata: Vec<u8>,
    pub foundry_counter: u32,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl AliasOutputRecord {
    /// Creates a new alias output row
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_timestamp: DateTime<Utc>,
        ms_range_id: u32,
        amount: Amount,
        sender: Address,
        issuer: Address,
        state_controller: Address,
        governor: Address,
        metadata: Option<MetadataFeatureBlock>,
        state_index: u32,
        state_metadata: Cursor<u8>,
        foundry_counter: u32,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            ms_timestamp,
            amount,
            native_tokens,
            state_index,
            state_metadata,
            state_controller,
            issuer,
            foundry_counter,
            governor,
            metadata,
            inclusion_state,
        }
    }
}

/// A `Foundry Output` table record
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct FoundryOutputRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_timestamp: DateTime<Utc>,
    pub ms_range_id: u32,
    pub amount: Amount,
    pub native_tokens: NativeTokens,
    pub serial_number: u32,
    pub token_tag: [u8; 12],
    pub circulating_supply: U256,
    pub max_supply: U256,
    pub token_schema: TokenScheme,
    pub alias_address: AliasId,
    pub metadata: Option<MetadataFeatureBlock>,
    pub immutable_metadata: Option<MetadataFeatureBlock>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl FoundryOutputRecord {
    /// Creates a new foundry output record
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_timestamp: DateTime<Utc>,
        ms_range_id: u32,
        alias_address: AliasAddress,
        amount: Amount,
        serial_number: u32,
        token_tag: [u8; 12],
        token_schema: TokenScheme,
        circulating_supply: U256,
        max_supply: U256,
        immutable_metadata: Option<MetadataFeatureBlock>,
        metadata: Option<MetadataFeatureBlock>,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            ms_timestamp,
            alias_address,
            amount,
            serial_number,
            token_tag,
            token_schema,
            immutable_metadata,
            metadata,
            circulating_supply,
            max_supply,
            native_tokens,
            inclusion_state,
        }
    }
}

/// An `NFT Output` table record
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct NftOutputRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_timestamp: DateTime<Utc>,
    pub ms_range_id: u32,
    pub amount: Amount,
    pub native_tokens: NativeTokens,
    pub address: NftId,
    pub unlock_conditions: UnlockConditions,
    pub sender: Option<SenderFeatureBlock>,
    pub metadata: Option<MetadataFeatureBlock>,
    pub tag: Option<TagFeatureBlock>,
    pub issuer: Option<IssuerFeatureBlock>,
    pub immutable_metadata: Option<MetadataFeatureBlock>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl NftOutputRecord {
    /// Creates a new foundry output record
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_timestamp: DateTime<Utc>,
        ms_range_id: u32,
        amount: Amount,
        native_tokens: NativeTokens,
        address: NftId,
        sender: Option<SenderFeatureBlock>,
        unlock_conditions: UnlockConditions,
        metadata: Option<MetadataFeatureBlock>,
        tag: Option<TagFeatureBlock>,
        issuer: Option<IssuerFeatureBlock>,
        immutable_metadata: Option<MetadataFeatureBlock>,
        metadata: Option<MetadataFeatureBlock>,
        inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            ms_timestamp,
            amount,
            address,
            unlock_conditions,
            immutable_metadata,
            metadata,
            inclusion_state,
            native_tokens,
            tag,
            issuer,
            sender,
        }
    }
}

/// A `tag` table row
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug)]
pub struct TagRecord {
    pub milestone_index: MilestoneIndex,
    pub ms_range_id: u32,
    pub ms_timestamp: DateTime<Utc>,
    pub message_id: MessageId,
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl TagRecord {
    /// Creates a new tag record
    pub fn new(
        milestone_index: MilestoneIndex,
        ms_range_id: u32,
        ms_timestamp: DateTime<Utc>,
        message_id: MessageId,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_range_id,
            message_id,
            ms_timestamp,
            ledger_inclusion_state,
        }
    }
}

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
