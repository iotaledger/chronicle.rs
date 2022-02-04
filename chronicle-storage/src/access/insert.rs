// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::access::types::*;
/////////////////// Messages tables ////////////////////////////
impl Insert<Bee<MessageId>, MessageRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.messages (message_id, message, version, milestone_index, inclusion_state, conflict_reason, proof) VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, message: &MessageRecord) -> B {
        builder
            .value(message_id)
            .value(Bee(message.message()))
            .value(message.version())
            .value(message.milestone_index().and_then(|m| Some(m.0)))
            .value(message.inclusion_state())
            .value(message.conflict_reason())
            .value(message.proof())
    }
}

impl Insert<Bee<MessageId>, Proof> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!("INSERT INTO #.messages (message_id, proof) VALUES (?, ?)", self.name())
    }
    fn bind_values<B: Binder>(builder: B, message_id: &Bee<MessageId>, proof: &Proof) -> B {
        builder.value(message_id).value(proof)
    }
}

/////////////////// Parents tables ////////////////////////////
impl Insert<Bee<MessageId>, ParentRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.parents (parent_id, milestone_index, ms_timestamp, message_id, inclusion_state)
            VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        parent_id: &Bee<MessageId>,
        ParentRecord {
            milestone_index,
            ms_timestamp,
            message_id,
            inclusion_state,
        }: &ParentRecord,
    ) -> B {
        builder
            .value(parent_id)
            .value(milestone_index.and_then(|m| Some(m.0)))
            .value(&ms_timestamp)
            .value(Bee(message_id))
            .value(inclusion_state)
    }
}

////////////////////// Tags Hint table /////////////////////////
impl Insert<TagHint, MsRangeId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.tags_hints (tag, variant, ms_range_id) VALUES (?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, hint: &Hint, ms_range_id: &MsRangeId) -> B {
        builder.value(hint.tag()).value(hint.variant()).value(&ms_range_id)
    }
}

////////////////////// Addresses Hint table /////////////////////////
impl Insert<AddressHint, MsRangeId> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.addresses_hints (address_type, address, output_kind, variant, ms_range_id) VALUES (?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(mut builder: B, hint: &Hint, ms_range_id: &MsRangeId) -> B {
        match self.address() {
            &Address::Ed25519(ed_address) => builder = builder.value(&"ed25519").value(Bee(ed_address)),
            &Address::Alias(alias_id) => builder = builder.value(&"alias").value(Bee(alias_id)),
            &Address::Nft(nft_id) => builder = builder.value(&"nft").value(Bee(nft_id)),
        }
        builder
            .value(hint.output_kind())
            .value(hint.variant())
            .value(&ms_range_id)
    }
}

///////////////////// Sync table ///////////////////////////
impl Insert<String, SyncRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.sync (key, milestone_index, synced_by, logged_by) VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        keyspace: &String,
        SyncRecord {
            milestone_index,
            synced_by,
            logged_by,
        }: &SyncRecord,
    ) -> B {
        builder
            .value(keyspace)
            .value(&Bee(milestone_index))
            .value(synced_by)
            .value(logged_by)
    }
}

////////////////////////////// Transactions table /////////////////////////////
/// Insert Transaction into Transactions table
/// Note: This can be used to store:
/// -input variant: (InputTransactionId, InputIndex) -> UTXOInput data column
/// -output variant: (OutputTransactionId, OutputIndex) -> Output data column
/// -unlock variant: (UtxoInputTransactionId, UtxoInputOutputIndex) -> Unlock data column
impl Insert<Bee<TransactionId>, TransactionRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.transactions (transaction_id, idx, variant, message_id, version, data, inclusion_state, milestone_index)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        transaction_id: &Bee<TransactionId>,
        transaction_record: &TransactionRecord,
    ) -> B {
        builder
            .value(transaction_id)
            .value(transaction_record.idx)
            .value(&transaction_record.variant)
            .value(&Bee(transaction_record.message_id))
            .value(&transaction_record.data)
            .value(&transaction_record.inclusion_state)
            .value(&transaction_record.milestone_index.and_then(|ms| Some(Bee(ms))))
    }
}

////////////////////// Outputs tables ////////////////////////////
/////////// Legacy output table /////////////

impl Insert<Bee<OutputId>, (Option<u32>, LegacyOutputRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.legacy_outputs (output_id, ms_range_id, ms_timestamp, milestone_index, output_type, address, amount, address_type, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        output_id: &Bee<OutputId>,
        (
            ttl,
            LegacyOutputRecord {
                milestone_index,
                ms_range_id,
                ms_timestamp,
                address,
                amount,
                inclusion_state,
                output_type,
            },
        ): &(Option<u32>, LegacyOutputRecord),
    ) -> B {
        builder
            .value(output_id)
            .value(ms_range_id)
            .value(ms_timestamp)
            .value(Bee(milestone_index))
            .value(output_type)
            .value(address)
            .value(amount)
            .value(&Ed25519Address::KIND)
            .value(inclusion_state)
            .value(ttl)
    }
}

impl Insert<Bee<OutputId>, (Option<u32>, BasicOutputRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.basic_outputs (output_id, ms_range_id, ms_timestamp, milestone_index, address,address_type,  sender, sender_address_type, amount, tag, metadata, unlock_conditions, native_tokens, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        output_id: &Bee<OutputId>,
        (
            ttl,
            BasicOutputRecord {
                milestone_index,
                ms_range_id,
                ms_timestamp,
                address,
                amount,
                inclusion_state,
                sender,
                native_tokens,
                tag,
                unlock_conditions,
                metadata,
            },
        ): &(Option<u32>, BasicOutputRecord),
    ) -> B {
        builder
            .value(output_id)
            .value(ms_range_id)
            .value(ms_timestamp)
            .value(Bee(milestone_index))
            .value(Bee(address))
            .value(address.kind())
            .value(sender.and_then(|s| Some(s.address().to_bech32("iota"))))
            .value(sender.and_then(|s| Some(s.address().kind())))
            .value(amount)
            .value(tag.and_then(|t| Some(t.to_string())))
            .value(metadata.and_then(|m| Some(Bee(m))))
            .value(unlock_conditions)
            .value(Bee(native_tokens))
            .value(inclusion_state)
            .value(ttl)
    }
}

impl Insert<Bee<AliasId>, (Option<u32>, AliasOutputRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.alias_outputs (alias_id, output_id, ms_range_id, ms_timestamp, milestone_index, sender,sender_address_type, amount, issuer, issuer_address_type, metadata, state_controller, state_controller_address_type, native_tokens, state_index, state_metadata, foundry_counter, governor, governor_address_type, unlock_conditions, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        alias_id: &Bee<AliasId>,
        (
            ttl,
            AliasOutputRecord {
                output_id,
                milestone_index,
                ms_range_id,
                ms_timestamp,
                address,
                amount,
                inclusion_state,
                output_type,
                sender,
                native_tokens,
                metadata,
                issuer,
                governor,
                state_index,
                state_metadata,
                state_controller,
                foundry_counter,
            },
        ): &(Option<u32>, AliasOutputRecord),
    ) -> B {
        builder
            .value(alias_id)
            .value(output_id)
            .value(ms_range_id)
            .value(ms_timestamp)
            .value(Bee(milestone_index))
            .value(sender.and_then(|s| Some(s.address().to_string())))
            .value(sender.and_then(|s| Some(s.address().kind())))
            .value(amount)
            .value(issuer.and_then(|s| Some(s.address().to_string())))
            .value(issuer.and_then(|s| Some(s.address().kind())))
            .value(metadata.and_then(|m| Some(Bee(m))))
            .value(state_controller.and_then(|s| Some(s.address().to_string())))
            .value(state_controller.and_then(|s| Some(s.address().kind())))
            .value(Bee(native_tokens))
            .value(state_index)
            .value(state_metadata)
            .value(foundry_counter)
            .value(governor.and_then(|s| Some(s.address().to_string())))
            .value(governor.and_then(|s| Some(s.address().kind())))
            .value(unlock_conditions)
            .value(inclusion_state)
            .value(ttl)
    }
}

impl Insert<Bee<FoundryId>, (Option<u32>, FoundryOutputRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.foundry_outputs (foundry_id, output_id, ms_range_id, ms_timestamp, milestone_index, token_tag, token_schema, alias_address, amount, metadata, immutable_metadata, native_tokens, serial_number, circulating_supply, max_supply, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        foundry_id: &Bee<FoundryId>,
        (
            ttl,
            FoundryOutputRecord {
                output_id,
                milestone_index,
                ms_range_id,
                ms_timestamp,
                alias_address,
                amount,
                serial_number,
                native_tokens,
                token_tag,
                token_schema,
                immutable_metadata,
                metadata,
                circulating_supply,
                max_supply,
                inclusion_state,
            },
        ): &(Option<u32>, FoundryOutputRecord),
    ) -> B {
        builder
            .value(foundry_id)
            .value(output_id)
            .value(ms_range_id)
            .value(ms_timestamp)
            .value(Bee(milestone_index))
            .value(token_tag)
            .value(Bee(token_schema))
            .value(Bee(alias_address))
            .value(amount)
            .value(metadata.and_then(|m| Some(Bee(m))))
            .value(immutable_metadata.and_then(|m| Some(Bee(m))))
            .value(Bee(native_tokens))
            .value(serial_number)
            .value(inclusion_state)
            .value(ttl)
    }
}

impl Insert<Bee<NftId>, (Option<u32>, NftOutputRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.nft_outputs (nft_id, output_id, ms_range_id, milestone_index, ms_timestamp, amount, inclusion_state, address, sender, sender_address_type, issuer, issuer_address_type, metadata, immutable_metadata, tag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        nft_id: &Bee<NftId>,
        NftOutputRecord {
            output_id,
            milestone_index,
            ms_range_id,
            ms_timestamp,
            address,
            amount,
            sender,
            issuer,
            unlock_conditions,
            native_tokens,
            tag,
            immutable_metadata,
            metadata,
            inclusion_state,
        }: &NftOutputRecord,
    ) -> B {
        builder
            .value(nft_id.0.to_string())
            .value(output_id.to_string())
            .value(ms_range_id)
            .value(Bee(milestone_index))
            .value(ms_timestamp)
            .value(amount)
            .value(inclusion_state)
            .value(address.to_string())
            .value(sender.and_then(|s| Some(s.address().to_string())))
            .value(sender.and_then(|s| Some(s.address().kind())))
            .value(issuer.and_then(|i| Some(i.address().to_string())))
            .value(issuer.and_then(|i| Some(i.address().kind())))
            .value(metadata.and_then(|m| Some(Bee(m))))
            .value(immutable_metadata.and_then(|m| Some(Bee(m))))
            .value(tag.and_then(|i| Some(t.to_string())))
    }
}

impl Insert<Partitioned<String>, (Option<u32>, TagRecord)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.tags (tag, ms_range_id, milestone_index, ms_timestamp, message_id, inclusion_state)
            VALUES (?, ?, ?, ?, ?, ?) USING TTL ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        Partitioned { inner, ms_range_id }: &Partitioned<String>,
        (
            ttl,
            TagRecord {
                milestone_index,
                ms_range_id,
                ms_timestamp,
                message_id,
                ledger_inclusion_state,
            },
        ): &(Option<u32>, TagRecord),
    ) -> B {
        builder
            .value(&inner)
            .value(ms_range_id)
            .value(Bee(milestone_index))
            .value(ms_timestamp)
            .value(Bee(message_id))
            .value(ledger_inclusion_state)
            .value(ttl)
    }
}

impl Insert<Bee<MilestoneIndex>, (Bee<MessageId>, Box<MilestonePayload>)> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!(
            "INSERT INTO #.milestones (milestone_index, message_id, timestamp, payload) VALUES (?, ?, ?, ?)",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        milestone_index: &Bee<MilestoneIndex>,
        (message_id, milestone_payload): &(Bee<MessageId>, Box<MilestonePayload>),
    ) -> B {
        builder
            .value(&milestone_index.0 .0)
            .value(message_id)
            .value(&milestone_payload.essence().timestamp())
            .value(&Bee(milestone_payload))
    }
}
