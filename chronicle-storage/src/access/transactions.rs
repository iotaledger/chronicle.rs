// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use bee_message::{
    input::{
        TreasuryInput,
        UtxoInput,
    },
    payload::{
        milestone::MilestoneId,
        receipt::ReceiptPayload,
    },
    unlock_block::UnlockBlock,
};

/// A `transactions` table row
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct TransactionRecord {
    pub transaction_id: TransactionId,
    pub idx: Index,
    pub variant: TransactionVariant,
    pub message_id: MessageId,
    pub data: TransactionData,
    pub milestone_index: Option<MilestoneIndex>,
    pub inclusion_state: Option<LedgerInclusionState>,
}

impl TransactionRecord {
    /// Creates an input transactions record
    pub fn input(
        transaction_id: TransactionId,
        idx: Index,
        message_id: MessageId,
        input_data: InputData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            transaction_id,
            idx,
            variant: TransactionVariant::Input,
            message_id,
            data: TransactionData::Input(input_data),
            inclusion_state,
            milestone_index,
        }
    }
    /// Creates an output transactions record
    pub fn output(
        transaction_id: TransactionId,
        idx: Index,
        message_id: MessageId,
        data: Output,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            transaction_id,
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
        transaction_id: TransactionId,
        idx: Index,
        message_id: MessageId,
        data: UnlockData,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        Self {
            transaction_id,
            idx,
            variant: TransactionVariant::Unlock,
            message_id,
            data: TransactionData::Unlock(data),
            inclusion_state,
            milestone_index,
        }
    }
    /// Creates a receipt transactions record
    pub fn receipt(
        milestone_id: MilestoneId,
        message_id: MessageId,
        data: ReceiptPayload,
        inclusion_state: Option<LedgerInclusionState>,
        milestone_index: Option<MilestoneIndex>,
    ) -> Self {
        let transaction_id = todo!("create it using milestone id");
        Self {
            transaction_id,
            // receipt always have zero as index.
            idx: 0,
            variant: TransactionVariant::Receipt,
            message_id,
            data: TransactionData::Receipt(data),
            inclusion_state,
            milestone_index,
        }
    }
    pub fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    pub fn idx(&self) -> &Index {
        &self.idx
    }

    pub fn variant(&self) -> &TransactionVariant {
        &self.variant
    }

    pub fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    pub fn data(&self) -> &TransactionData {
        &self.data
    }

    pub fn milestone_index(&self) -> &Option<MilestoneIndex> {
        &self.milestone_index
    }

    pub fn inclusion_state(&self) -> &Option<LedgerInclusionState> {
        &self.inclusion_state
    }
}
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
    /// A Receipt payload, used to point to treasury payload
    Receipt = 3,
}

/// A transaction's unlock data, to be stored in a `transactions` row.
/// Holds a reference to the input which it signs.
#[derive(Debug, Clone, Packable)]
#[packable(unpack_error = anyhow::Error)]
pub struct UnlockData {
    /// it holds the transaction_id of the input which created the unlock_block
    pub input_tx_id: TransactionId,
    /// it holds the input_index of the input which created the unlock_block
    pub input_index: Index,
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

/// A transaction's input data, to be stored in a `transactions` row.
#[derive(Debug, Clone, Packable)]
#[packable(unpack_error = anyhow::Error)]
#[packable(tag_type = u8, with_error = (|tag| anyhow::anyhow!("Invalid input tag: {}", tag)))]
pub enum InputData {
    /// An regular Input which spends a prior Output and its unlock block
    #[packable(tag = 0)]
    Utxo(UtxoInput, UnlockBlock),
    /// A special input for migrating funds from another network
    #[packable(tag = 1)]
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

#[derive(Debug, Clone, Packable)]
#[packable(unpack_error = anyhow::Error)]
#[packable(tag_type = u8, with_error = (|tag| anyhow::anyhow!("Invalid transaction type: {}", tag)))]
/// Chrysalis transaction data
pub enum TransactionData {
    /// An unspent transaction input
    #[packable(tag = 0)]
    Input(InputData),
    /// A transaction output
    #[packable(tag = 1)]
    Output(Output),
    /// A signed block which can be used to unlock an input
    #[packable(tag = 2)]
    Unlock(UnlockData),
    /// A receipt payload which can be used to trace back to migrated/treasury funds
    #[packable(tag = 3)]
    Receipt(bee_message::payload::ReceiptPayload),
}

/// A result struct which holds a retrieved transaction
#[derive(Debug, Clone)]
pub struct TransactionRes {
    /// The transaction's message id
    pub message_id: MessageId,
    /// The transaction's milestone index
    pub milestone_index: Option<MilestoneIndex>,
    /// The output
    pub outputs: Vec<(Output, Option<UnlockRes>)>,
    /// The inputs, if any exist
    pub inputs: Vec<InputData>,
    /// the receipts, if any exist
    pub receipts: Vec<ReceiptPayload>,
}

/// A result struct which holds an unlock row from the `transactions` table
#[derive(Debug, Clone)]
pub struct UnlockRes {
    /// The message ID for the transaction which this unlocks
    pub message_id: MessageId,
    /// The unlock block
    pub block: UnlockBlock,
    /// This transaction's ledger inclusion state
    pub inclusion_state: Option<LedgerInclusionState>,
}
