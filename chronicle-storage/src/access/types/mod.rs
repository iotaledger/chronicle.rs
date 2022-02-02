use super::*;
use bee_message_v1::prelud::ConflictReason;
use chronicle_common::Wrapper;
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
}

impl ColumnDecoder for TransactionVariant {
    fn try_decode_column(slice: &[u8]) -> anyhow::Result<Self> {
        Ok(match std::str::from_utf8(slice)? {
            "input" => TransactionVariant::Input,
            "output" => TransactionVariant::Output,
            "unlock" => TransactionVariant::Unlock,
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
        self.message_id
    }
    /// Return the message
    pub fn message(&self) -> &Message {
        self.message
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
    pub ms_timestamp: Option<chrono::DateTime>,
    pub message_id: MessageId,
    pub ledger_inclusion_state: Option<LedgerInclusionState>,
}

impl ParentRecord {
    /// Creates a new parent row
    pub fn new(
        milestone_index: Option<MilestoneIndex>,
        ms_timestamp: Option<chrono::DateTime>,
        message_id: MessageId,
        ledger_inclusion_state: Option<LedgerInclusionState>,
    ) -> Self {
        Self {
            milestone_index,
            ms_timestamp,
            message_id,
            ledger_inclusion_state,
        }
    }
}

/// A hint, used to lookup in the `hints` table
#[derive(Clone, Debug)]
pub struct Hint {
    /// The hint string
    pub hint: String,
    /// The hint variant. Can be 'parent', 'address', or 'index'.
    pub variant: HintVariant,
}

impl Hint {
    /// Creates a new index hint
    pub fn index(index: String) -> Self {
        Self {
            hint: index,
            variant: HintVariant::Index,
        }
    }

    /// Creates a new address hint
    pub fn address(address: String) -> Self {
        Self {
            hint: address,
            variant: HintVariant::Address,
        }
    }

    /// Creates a new parent hint
    pub fn parent(parent: String) -> Self {
        Self {
            hint: parent,
            variant: HintVariant::Parent,
        }
    }
}

/// Hint variants
#[derive(Clone, Debug)]
pub enum HintVariant {
    /// An address
    Address,
    /// An unhashed index
    Index,
    /// A parent message id
    Parent,
}

impl std::fmt::Display for HintVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                HintVariant::Address => "address",
                HintVariant::Index => "index",
                HintVariant::Parent => "parent",
            }
        )
    }
}

impl ColumnEncoder for HintVariant {
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.to_string().encode(buffer)
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
            data: TransactionData::Unlock(data),
            inclusion_state,
            milestone_index,
        }
    }
}

pub mod v1;
pub mod v2;
pub mod versioned;
