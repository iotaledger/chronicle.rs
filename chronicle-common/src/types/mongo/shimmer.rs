// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use anyhow::{
    anyhow,
    bail,
};
use mongodb::bson::{
    from_bson,
    to_bson,
    Bson,
    DateTime,
    Document,
};
use primitive_types::U256;
use serde_json::json;
use std::str::FromStr;

pub fn message_to_bson(message: &crate::shimmer::Message) -> Bson {
    let mut doc = Document::new();
    doc.insert("protocol_version", message.protocol_version() as i32);
    doc.insert(
        "parents",
        message.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("payload", message.payload().map(|p| payload_to_bson(p)));
    doc.insert("nonce", message.nonce().to_string());
    Bson::Document(doc)
}

pub fn message_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::Message> {
    let value = json!({
        "protocol_version": doc.get_i32("protocol_version")?,
        "parents": doc.get_array("parents")?.iter().map(|p| message_id_from_bson(p)).collect::<Result<Vec<_>,_>>()?,
        "payload": payload_from_doc(doc.get_document("payload")?)?,
        "nonce": doc.get_str("nonce")?.parse::<u64>()?,
    });
    Ok(serde_json::from_value(value)?)
}

fn payload_to_bson(payload: &crate::shimmer::payload::Payload) -> Bson {
    use crate::shimmer::payload::Payload;
    match payload {
        Payload::Transaction(t) => transaction_payload_to_bson(&**t),
        Payload::Milestone(m) => milestone_payload_to_bson(&**m),
        Payload::Receipt(r) => receipt_payload_to_bson(&**r),
        Payload::TreasuryTransaction(t) => treasury_payload_to_bson(&**t),
        Payload::TaggedData(t) => tag_payload_to_bson(&**t),
    }
}

pub fn payload_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::payload::Payload> {
    use crate::shimmer::payload::{
        milestone::MilestonePayload,
        receipt::ReceiptPayload,
        tagged_data::TaggedDataPayload,
        transaction::TransactionPayload,
        treasury_transaction::TreasuryTransactionPayload,
        Payload,
    };
    let kind = doc.get_i32("kind")? as u32;
    Ok(match kind {
        TransactionPayload::KIND => Payload::Transaction(Box::new(transaction_payload_from_doc(doc)?)),
        MilestonePayload::KIND => Payload::Milestone(Box::new(milestone_payload_from_doc(doc)?)),
        TaggedDataPayload::KIND => Payload::TaggedData(Box::new(tag_payload_from_doc(doc)?)),
        ReceiptPayload::KIND => Payload::Receipt(Box::new(receipt_payload_from_doc(doc)?)),
        TreasuryTransactionPayload::KIND => Payload::TreasuryTransaction(Box::new(treasury_payload_from_doc(doc)?)),
        _ => bail!("Unknown payload kind: {}", kind),
    })
}

fn transaction_payload_to_bson(payload: &crate::shimmer::payload::transaction::TransactionPayload) -> Bson {
    use crate::shimmer::payload::transaction::TransactionPayload;
    let mut doc = Document::new();
    doc.insert("kind", TransactionPayload::KIND as i32);
    doc.insert("transaction_id", payload.id().to_string());
    doc.insert("essence", transaction_essence_to_bson(&payload.essence()));
    doc.insert(
        "unlock_blocks",
        payload
            .unlock_blocks()
            .iter()
            .map(|u| unlock_block_to_bson(u))
            .collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn transaction_payload_from_doc(
    doc: &Document,
) -> anyhow::Result<crate::shimmer::payload::transaction::TransactionPayload> {
    Ok(serde_json::from_value(json!({
        "essence": transaction_essence_from_doc(doc.get_document("essence")?)?,
        "unlock_blocks": doc.get_array("unlock_blocks")?.iter().map(|u| unlock_block_from_bson(u)).collect::<Result<Vec<_>,_>>()?,
    }))?)
}

fn milestone_payload_to_bson(payload: &crate::shimmer::payload::milestone::MilestonePayload) -> Bson {
    use crate::shimmer::payload::milestone::MilestonePayload;
    let mut doc = Document::new();
    doc.insert("kind", MilestonePayload::KIND as i32);
    doc.insert("milestone_id", payload.id().to_string());
    doc.insert("essence", milestone_essence_to_bson(&payload.essence()));
    doc.insert(
        "signatures",
        payload.signatures().map(|s| hex::encode(s)).collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn milestone_payload_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::payload::milestone::MilestonePayload> {
    Ok(serde_json::from_value(json!({
        "essence": milestone_essence_from_doc(doc.get_document("essence")?)?,
        "signatures": doc.get_array("signatures")?.iter().map(|u| bytes_from_bson(u)).collect::<Result<Vec<_>,_>>()?,
    }))?)
}

fn tag_payload_to_bson(payload: &crate::shimmer::payload::tagged_data::TaggedDataPayload) -> Bson {
    use crate::shimmer::payload::tagged_data::TaggedDataPayload;
    let mut doc = Document::new();
    doc.insert("kind", TaggedDataPayload::KIND as i32);
    doc.insert("tag", hex::encode(payload.tag()));
    doc.insert("data", hex::encode(payload.data()));
    Bson::Document(doc)
}

fn tag_payload_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::payload::tagged_data::TaggedDataPayload> {
    Ok(serde_json::from_value(json!({
        "tag": hex::decode(doc.get_str("tag")?)?,
        "data": hex::decode(doc.get_str("data")?)?,
    }))?)
}

fn receipt_payload_to_bson(payload: &crate::shimmer::payload::receipt::ReceiptPayload) -> Bson {
    use crate::shimmer::payload::receipt::ReceiptPayload;
    let mut doc = Document::new();
    doc.insert("kind", ReceiptPayload::KIND as i32);
    doc.insert("migrated_at", payload.migrated_at().0 as i32);
    doc.insert("last", payload.last());
    doc.insert(
        "funds",
        payload
            .funds()
            .iter()
            .map(|f| {
                let mut doc = Document::new();
                doc.insert("tail_transaction_hash", hex::encode(f.tail_transaction_hash()));
                doc.insert("address", address_to_bson(f.address()));
                doc.insert("amount", f.amount() as i64);
                doc
            })
            .collect::<Vec<_>>(),
    );
    doc.insert("transaction", payload_to_bson(payload.transaction()));
    doc.insert("amount", payload.amount() as i64);
    Bson::Document(doc)
}

fn receipt_payload_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::payload::receipt::ReceiptPayload> {
    Ok(serde_json::from_value(json!({
        "migrated_at": doc.get_i32("migrated_at")?,
        "last": doc.get_bool("last")?,
        "funds": doc.get_array("funds")?.iter().map(|f| migrated_funds_entry_from_bson(f)).collect::<Result<Vec<_>,_>>()?,
        "transaction": payload_from_doc(doc.get_document("transaction")?)?,
        "amount": doc.get_i64("amount")?,
    }))?)
}

fn treasury_payload_to_bson(
    payload: &crate::shimmer::payload::treasury_transaction::TreasuryTransactionPayload,
) -> Bson {
    use crate::shimmer::payload::treasury_transaction::TreasuryTransactionPayload;
    let mut doc = Document::new();
    doc.insert("kind", TreasuryTransactionPayload::KIND as i32);
    doc.insert("input", input_to_bson(payload.input()));
    doc.insert("output", output_to_bson(payload.output()));
    Bson::Document(doc)
}

fn treasury_payload_from_doc(
    doc: &Document,
) -> anyhow::Result<crate::shimmer::payload::treasury_transaction::TreasuryTransactionPayload> {
    Ok(serde_json::from_value(json!({
        "input": input_from_doc(doc.get_document("input")?)?,
        "output": output_from_doc(doc.get_document("output")?)?,
    }))?)
}

fn transaction_essence_to_bson(essence: &crate::shimmer::payload::transaction::TransactionEssence) -> Bson {
    use crate::shimmer::payload::transaction::TransactionEssence;
    let mut doc = Document::new();
    match essence {
        TransactionEssence::Regular(r) => {
            doc.insert("network_id", r.network_id() as i64);
            doc.insert(
                "inputs",
                r.inputs().iter().map(|i| input_to_bson(i)).collect::<Vec<_>>(),
            );
            doc.insert("inputs_commitment", hex::encode(r.inputs_commitment()));
            doc.insert(
                "outputs",
                r.outputs().iter().map(|o| output_to_bson(o)).collect::<Vec<_>>(),
            );
            doc.insert("payload", r.payload().as_ref().map(|p| payload_to_bson(p)));
        }
    }
    Bson::Document(doc)
}

fn transaction_essence_from_doc(
    doc: &Document,
) -> anyhow::Result<crate::shimmer::payload::transaction::TransactionEssence> {
    Ok(serde_json::from_value(json!({
        "network_id": doc.get_i64("network_id")?,
        "inputs": doc.get_array("inputs")?.iter().map(|i| input_from_bson(i)).collect::<Result<Vec<_>,_>>()?,
        "outputs": doc.get_array("outputs")?.iter().map(|o| output_from_bson(o)).collect::<Result<Vec<_>,_>>()?,
        "payload": payload_from_doc(doc.get_document("payload")?)?
    }))?)
}

fn milestone_essence_to_bson(essence: &crate::shimmer::payload::milestone::MilestoneEssence) -> Bson {
    let mut doc = Document::new();
    doc.insert("index", essence.index().0 as i32);
    doc.insert("timestamp", DateTime::from_millis(essence.timestamp() as i64));
    doc.insert(
        "parents",
        essence.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("merkle_proof", hex::encode(essence.merkle_proof()));
    doc.insert("next_pow_score", essence.next_pow_score() as i64);
    doc.insert(
        "next_pow_score_milestone_index",
        essence.next_pow_score_milestone_index() as i32,
    );
    doc.insert(
        "public_keys",
        essence.public_keys().iter().map(|p| hex::encode(p)).collect::<Vec<_>>(),
    );
    doc.insert("receipt", essence.receipt().map(|p| payload_to_bson(p)));
    Bson::Document(doc)
}

fn milestone_essence_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::payload::milestone::MilestoneEssence> {
    Ok(serde_json::from_value(json!({
        "index": doc.get_i32("index")?,
        "timestamp": doc.get_datetime("timestamp")?.timestamp_millis(),
        "parents": doc.get_array("parents")?.iter().map(|p| message_id_from_bson(p)).collect::<Result<Vec<_>,_>>()?,
        "merkle_proof": hex::decode(doc.get_str("merkle_proof")?)?,
        "next_pow_score": doc.get_i64("next_pow_score")?,
        "next_pow_score_milestone_index": doc.get_i32("next_pow_score_milestone_index")?,
        "public_keys": doc.get_array("public_keys")?.iter().map(|p| bytes_from_bson(p)).collect::<Result<Vec<_>,_>>()?,
        "receipt": payload_from_doc(doc.get_document("receipt")?)?
    }))?)
}

fn unlock_block_to_bson(unlock_block: &crate::shimmer::unlock_block::UnlockBlock) -> Bson {
    use crate::shimmer::{
        signature::Signature,
        unlock_block::{
            AliasUnlockBlock,
            NftUnlockBlock,
            ReferenceUnlockBlock,
            SignatureUnlockBlock,
            UnlockBlock,
        },
    };
    let mut doc = Document::new();
    match unlock_block {
        UnlockBlock::Signature(s) => match s.signature() {
            Signature::Ed25519(s) => {
                doc.insert("kind", SignatureUnlockBlock::KIND as i32);
                doc.insert("public_key", hex::encode(s.public_key()));
                doc.insert("signature", hex::encode(s.signature()));
            }
        },
        UnlockBlock::Reference(r) => {
            doc.insert("kind", ReferenceUnlockBlock::KIND as i32);
            doc.insert("index", r.index() as i32);
        }
        UnlockBlock::Alias(a) => {
            doc.insert("kind", AliasUnlockBlock::KIND as i32);
            doc.insert("index", a.index() as i32);
        }
        UnlockBlock::Nft(n) => {
            doc.insert("kind", NftUnlockBlock::KIND as i32);
            doc.insert("index", n.index() as i32);
        }
    }
    Bson::Document(doc)
}

fn unlock_block_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::unlock_block::UnlockBlock> {
    use crate::shimmer::unlock_block::{
        AliasUnlockBlock,
        NftUnlockBlock,
        ReferenceUnlockBlock,
        SignatureUnlockBlock,
    };
    let doc = bson.as_document().ok_or_else(|| anyhow!("Invalid unlock block"))?;
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        SignatureUnlockBlock::KIND => serde_json::from_value(json!({
            "type": "Signature",
            "data": {
                "type": "Ed25519",
                "data": {
                    "public_key": hex::decode(doc.get_str("public_key")?)?,
                    "signature": hex::decode(doc.get_str("signature")?)?,
                }
            }
        }))?,
        ReferenceUnlockBlock::KIND => serde_json::from_value(json!({
            "type": "Reference",
            "data": {
                "index": doc.get_i32("index")?
            }
        }))?,
        AliasUnlockBlock::KIND => serde_json::from_value(json!({
            "type": "Alias",
            "data": {
                "index": doc.get_i32("index")?
            }
        }))?,
        NftUnlockBlock::KIND => serde_json::from_value(json!({
            "type": "Nft",
            "data": {
                "index": doc.get_i32("index")?
            }
        }))?,
        _ => bail!("Invalid unlock block"),
    })
}

fn input_to_bson(input: &crate::shimmer::input::Input) -> Bson {
    use crate::shimmer::input::{
        Input,
        TreasuryInput,
        UtxoInput,
    };
    let mut doc = Document::new();
    match input {
        Input::Utxo(u) => {
            doc.insert("kind", UtxoInput::KIND as i32);
            doc.insert("transaction_id", u.output_id().transaction_id().to_string());
            doc.insert("index", u.output_id().index() as i32);
        }
        Input::Treasury(t) => {
            doc.insert("kind", TreasuryInput::KIND as i32);
            doc.insert("milestone_id", t.milestone_id().to_string());
        }
    }
    Bson::Document(doc)
}

fn input_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::input::Input> {
    input_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid input"))?)
}

fn input_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::input::Input> {
    use crate::shimmer::input::{
        TreasuryInput,
        UtxoInput,
    };
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        UtxoInput::KIND => serde_json::from_value(json!({
            "type": "Utxo",
            "transaction_id": doc.get_str("transaction_id")?.to_string(),
            "index": doc.get_i32("index")?
        }))?,
        TreasuryInput::KIND => serde_json::from_value(json!({
            "type": "Treasury",
            "milestone_id": doc.get_str("milestone_id")?.to_string(),
        }))?,
        _ => bail!("Invalid input"),
    })
}

fn output_to_bson(output: &crate::shimmer::output::Output) -> Bson {
    use crate::shimmer::output::{
        AliasOutput,
        BasicOutput,
        FoundryOutput,
        NftOutput,
        Output,
        TreasuryOutput,
    };
    let mut doc = Document::new();
    match output {
        Output::Treasury(t) => {
            doc.insert("kind", TreasuryOutput::KIND as i32);
            doc.insert("amount", t.amount() as i64);
        }
        Output::Basic(b) => {
            doc.insert("kind", BasicOutput::KIND as i32);
            doc.insert("amount", b.amount() as i64);
            doc.insert("native_tokens", to_bson(b.native_tokens()).unwrap());
            doc.insert("unlock_conditions", to_bson(b.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(b.feature_blocks()).unwrap());
        }
        Output::Alias(a) => {
            doc.insert("kind", AliasOutput::KIND as i32);
            doc.insert("amount", a.amount() as i64);
            doc.insert("native_tokens", to_bson(a.native_tokens()).unwrap());
            doc.insert("alias_id", a.alias_id().to_string());
            doc.insert("state_index", a.state_index() as i32);
            doc.insert("state_metadata", hex::encode(a.state_metadata()));
            doc.insert("foundry_counter", a.foundry_counter() as i32);
            doc.insert("unlock_conditions", to_bson(a.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(a.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(a.immutable_feature_blocks()).unwrap(),
            );
        }
        Output::Foundry(f) => {
            doc.insert("kind", FoundryOutput::KIND as i32);
            doc.insert("amount", f.amount() as i64);
            doc.insert("native_tokens", to_bson(f.native_tokens()).unwrap());
            doc.insert("serial_number", f.serial_number() as i32);
            doc.insert("token_tag", f.token_tag().to_string());
            doc.insert("minted_tokens", f.minted_tokens().to_string());
            doc.insert("melted_tokens", f.melted_tokens().to_string());
            doc.insert("maximum_supply", f.maximum_supply().to_string());
            doc.insert("token_scheme", f.token_scheme() as u8 as i32);
            doc.insert("unlock_conditions", to_bson(f.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(f.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(f.immutable_feature_blocks()).unwrap(),
            );
        }
        Output::Nft(n) => {
            doc.insert("kind", NftOutput::KIND as i32);
            doc.insert("amount", n.amount() as i64);
            doc.insert("native_tokens", to_bson(n.native_tokens()).unwrap());
            doc.insert("nft_id", n.nft_id().to_string());
            doc.insert("unlock_conditions", to_bson(n.unlock_conditions()).unwrap());
            doc.insert("feature_blocks", to_bson(n.feature_blocks()).unwrap());
            doc.insert(
                "immutable_feature_blocks",
                to_bson(n.immutable_feature_blocks()).unwrap(),
            );
        }
    }
    Bson::Document(doc)
}

fn output_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::output::Output> {
    output_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid output"))?)
}

fn output_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::output::Output> {
    use crate::shimmer::output::{
        AliasOutput,
        BasicOutput,
        FoundryOutput,
        NftOutput,
        TreasuryOutput,
    };
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        TreasuryOutput::KIND => serde_json::from_value(json!({
            "type": "Treasury",
            "data": {
                "amount": doc.get_i64("amount")?
            }
        }))?,
        BasicOutput::KIND => serde_json::from_value(json!({
            "type": "Basic",
            "data": {
                "amount": doc.get_i64("amount")?,
                "native_tokens": from_bson(doc.get("native_tokens").ok_or_else(|| anyhow!("Missing native tokens"))?.clone())?,
                "unlock_conditions": from_bson(doc.get("unlock_conditions").ok_or_else(|| anyhow!("Missing unlock conditions"))?.clone())?,
                "feature_blocks": from_bson(doc.get("feature_blocks").ok_or_else(|| anyhow!("Missing feature blocks"))?.clone())?,
            }
        }))?,
        AliasOutput::KIND => serde_json::from_value(json!({
            "type": "Alias",
            "data": {
                "amount": doc.get_i64("amount")?,
                "native_tokens": from_bson(doc.get("native_tokens").ok_or_else(|| anyhow!("Missing native tokens"))?.clone())?,
                "alias_id": doc.get_str("alias_id")?,
                "state_index": doc.get_i32("state_index")?,
                "state_metadata": hex::decode(doc.get_str("state_metadata")?)?,
                "foundry_counter": doc.get_i32("foundry_counter")?,
                "unlock_conditions": from_bson(doc.get("unlock_conditions").ok_or_else(|| anyhow!("Missing unlock conditions"))?.clone())?,
                "feature_blocks": from_bson(doc.get("feature_blocks").ok_or_else(|| anyhow!("Missing feature blocks"))?.clone())?,
                "immutable_feature_blocks": from_bson(doc.get("immutable_feature_blocks").ok_or_else(|| anyhow!("Missing immutable feature blocks"))?.clone())?,
            }
        }))?,
        FoundryOutput::KIND => serde_json::from_value(json!({
            "type": "Foundry",
            "data": {
                "amount": doc.get_i64("amount")?,
                "native_tokens": from_bson(doc.get("native_tokens").ok_or_else(|| anyhow!("Missing native tokens"))?.clone())?,
                "serial_number": doc.get_i32("serial_number")?,
                "token_tag": doc.get_str("token_tag")?,
                "minted_tokens": doc.get_str("minted_tokens")?.parse::<U256>()?,
                "melted_tokens": doc.get_str("melted_tokens")?.parse::<U256>()?,
                "maximum_supply": doc.get_str("maximum_supply")?.parse::<U256>()?,
                "token_scheme": doc.get_i32("token_scheme")? as u8,
                "unlock_conditions": from_bson(doc.get("unlock_conditions").ok_or_else(|| anyhow!("Missing unlock conditions"))?.clone())?,
                "feature_blocks": from_bson(doc.get("feature_blocks").ok_or_else(|| anyhow!("Missing feature blocks"))?.clone())?,
                "immutable_feature_blocks": from_bson(doc.get("immutable_feature_blocks").ok_or_else(|| anyhow!("Missing immutable feature blocks"))?.clone())?,
            }
        }))?,
        NftOutput::KIND => serde_json::from_value(json!({
            "type": "Nft",
            "data": {
                "amount": doc.get_i64("amount")?,
                "native_tokens": from_bson(doc.get("native_tokens").ok_or_else(|| anyhow!("Missing native tokens"))?.clone())?,
                "nft_id": doc.get_str("nft_id")?,
                "unlock_conditions": from_bson(doc.get("unlock_conditions").ok_or_else(|| anyhow!("Missing unlock conditions"))?.clone())?,
                "feature_blocks": from_bson(doc.get("feature_blocks").ok_or_else(|| anyhow!("Missing feature blocks"))?.clone())?,
                "immutable_feature_blocks": from_bson(doc.get("immutable_feature_blocks").ok_or_else(|| anyhow!("Missing immutable feature blocks"))?.clone())?,
            }
        }))?,
        _ => bail!("Invalid output"),
    })
}

fn address_to_bson(address: &crate::shimmer::address::Address) -> Bson {
    use crate::shimmer::address::{
        Address,
        AliasAddress,
        Ed25519Address,
        NftAddress,
    };
    let mut doc = Document::new();
    match address {
        Address::Ed25519(a) => {
            doc.insert("kind", Ed25519Address::KIND as i32);
            doc.insert("data", a.to_string());
        }
        Address::Alias(a) => {
            doc.insert("kind", AliasAddress::KIND as i32);
            doc.insert("data", a.alias_id().to_string());
        }
        Address::Nft(n) => {
            doc.insert("kind", NftAddress::KIND as i32);
            doc.insert("data", n.nft_id().to_string());
        }
    }
    Bson::Document(doc)
}

fn address_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::address::Address> {
    address_from_doc(bson.as_document().ok_or_else(|| anyhow!("Invalid address"))?)
}

fn address_from_doc(doc: &Document) -> anyhow::Result<crate::shimmer::address::Address> {
    use crate::shimmer::address::{
        AliasAddress,
        Ed25519Address,
        NftAddress,
    };
    let kind = doc.get_i32("kind")? as u8;
    Ok(match kind {
        Ed25519Address::KIND => serde_json::from_value(json!({
            "type": "Ed25519",
            "data": doc.get_str("data")?.to_string()
        }))?,
        AliasAddress::KIND => serde_json::from_value(json!({
            "type": "Alias",
            "data": doc.get_str("data")?.to_string()
        }))?,
        NftAddress::KIND => serde_json::from_value(json!({
            "type": "Nft",
            "data": doc.get_str("data")?.to_string()
        }))?,
        _ => bail!("Invalid address"),
    })
}

fn bytes_from_bson(bson: &Bson) -> anyhow::Result<Vec<u8>> {
    let hex = bson.as_str().ok_or_else(|| anyhow!("Invalid bytes hex"))?;
    Ok(hex::decode(hex)?)
}

fn message_id_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::MessageId> {
    let s = bson.as_str().ok_or_else(|| anyhow!("Invalid message id"))?;
    Ok(crate::shimmer::MessageId::from_str(s)?)
}

fn migrated_funds_entry_from_bson(bson: &Bson) -> anyhow::Result<crate::shimmer::payload::receipt::MigratedFundsEntry> {
    let doc = bson.as_document().ok_or_else(|| anyhow!("Invalid funds"))?;
    Ok(serde_json::from_value(json!({
        "tail_transaction_hash": hex::decode(doc.get_str("tail_transaction_hash")?)?,
        "address": address_from_bson(doc.get("address").ok_or_else(|| anyhow!("Missing address"))?)?,
        "amount": doc.get_i64("amount")?,
    }))?)
}
