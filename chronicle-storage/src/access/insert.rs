// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

/////////////////// Messages tables ////////////////////////////
impl Insert<MessageRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (
                message_id, 
                message, 
                version, 
                milestone_index, 
                inclusion_state, 
                conflict_reason, 
                proof
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message: &MessageRecord, _: &()) -> B {
        builder.bind(message)
    }
}

impl Insert<Bee<MessageId>, Proof> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (
                message_id, 
                proof
            ) 
            VALUES (?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, proof: &Proof) -> B {
        builder.value(message_id).value(proof)
    }
}

/////////////////// Parents tables ////////////////////////////
impl Insert<ParentRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.parents (
                parent_id, 
                milestone_index, 
                ms_timestamp, 
                message_id, 
                inclusion_state
            )
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, parent: &ParentRecord, _: &()) -> B {
        builder.bind(parent)
    }
}

////////////////////// Tags Hint table /////////////////////////
impl Insert<TagHint, MsRangeId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.tag_hints (
                tag, 
                table_kind, 
                ms_range_id
            ) 
            VALUES (?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, hint: &TagHint, ms_range_id: &MsRangeId) -> B {
        builder.value(hint.tag()).value(hint.table_kind()).value(ms_range_id)
    }
}

////////////////////// Addresses Hint table /////////////////////////
impl Insert<AddressHint, MsRangeId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.addresses_hints (
                address, 
                output_kind, 
                variant, 
                ms_range_id
            ) 
            VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, hint: &AddressHint, ms_range_id: &MsRangeId) -> B {
        builder
            .value(Bee(hint.address()))
            .value(hint.output_kind())
            .value(hint.variant())
            .value(&ms_range_id)
    }
}

///////////////////// Sync table ///////////////////////////
impl Insert<SyncRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.sync (
                ms_range_id, 
                milestone_index, 
                synced_by, 
                logged_by
            )
            VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, sync: &SyncRecord, _: &()) -> B {
        builder.bind(sync)
    }
}

////////////////////////////// Transactions table /////////////////////////////
/// Insert Transaction into Transactions table
/// Note: This can be used to store:
/// -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
/// -output variant: (OutputTransactionId, OutputIndex) -> Output data column
/// -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
impl Insert<TransactionRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.transactions (
                transaction_id, 
                idx, 
                variant, 
                message_id, 
                version, 
                data, 
                inclusion_state, 
                milestone_index
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, transaction: &TransactionRecord, _: &()) -> B {
        builder.bind(transaction)
    }
}

////////////////////// Outputs tables ////////////////////////////
/////////// Legacy output table /////////////

impl Insert<LegacyOutputRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.legacy_outputs (
                output_id,
                output_type,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                inclusion_state,
                address,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output: &LegacyOutputRecord, ttl: &Option<u32>) -> B {
        builder.bind(output).value(ttl)
    }
}

impl Insert<BasicOutputRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.basic_outputs (
                output_id,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                inclusion_state,
                address,
                sender,
                tag,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output: &BasicOutputRecord, ttl: &Option<u32>) -> B {
        builder.bind(output).value(ttl)
    }
}

impl Insert<AliasOutputRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.alias_outputs (
                alias_id,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                inclusion_state,
                sender,
                issuer,
                state_controller,
                governor,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output: &AliasOutputRecord, ttl: &Option<u32>) -> B {
        builder.bind(output).value(ttl)
    }
}

impl Insert<FoundryOutputRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.foundry_outputs (
                foundry_id,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                inclusion_state,
                address,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output: &FoundryOutputRecord, ttl: &Option<u32>) -> B {
        builder.bind(output).value(ttl)
    }
}

impl Insert<NftOutputRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.nft_outputs (
                nft_id,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                inclusion_state,
                address,
                dust_return_address,
                sender,
                issuer,
                tag,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output: &NftOutputRecord, ttl: &Option<u32>) -> B {
        builder.bind(output).value(ttl)
    }
}

impl Insert<TagRecord, Option<u32>> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.tags (
                tag,
                ms_range_id,
                milestone_index,
                ms_timestamp,
                message_id,
                inclusion_state
            )
            VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, tag: &TagRecord, ttl: &Option<u32>) -> B {
        builder.bind(tag).value(ttl)
    }
}

impl Insert<MilestoneRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.milestones (
                milestone_index, 
                message_id, 
                timestamp, 
                payload
            ) 
            VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, milestone: &MilestoneRecord, _: &()) -> B {
        builder.bind(milestone)
    }
}

impl Insert<MsAnalyticsRecord, ()> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.ms_analytics (
                ms_range_id, 
                milestone_index, 
                message_count, 
                transaction_count,
                transferred_tokens
            ) 
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, rec: &MsAnalyticsRecord, _: &()) -> B {
        builder.bind(rec)
    }
}
