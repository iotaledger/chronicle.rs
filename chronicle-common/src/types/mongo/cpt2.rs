// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use mongodb::bson::{
    doc,
    Bson,
    DateTime,
    Document,
};

pub fn message_to_bson(message: &crate::cpt2::Message) -> Bson {
    let mut doc = Document::new();
    doc.insert("protocol_version", 0);
    doc.insert("network_id", message.network_id() as i64);
    doc.insert(
        "parents",
        message.parents().iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    );
    doc.insert("payload", message.payload().as_ref().map(|p| payload_to_bson(p)));
    doc.insert("nonce", message.nonce().to_string());
    Bson::Document(doc)
}

fn payload_to_bson(payload: &crate::cpt2::payload::Payload) -> Bson {
    use crate::cpt2::payload::Payload;
    match payload {
        Payload::Transaction(t) => transaction_payload_to_bson(&**t),
        Payload::Milestone(m) => milestone_payload_to_bson(&**m),
        Payload::Indexation(i) => indexation_payload_to_bson(&**i),
        Payload::Receipt(r) => receipt_payload_to_bson(&**r),
        Payload::TreasuryTransaction(t) => treasury_payload_to_bson(&**t),
    }
}

fn transaction_payload_to_bson(payload: &crate::cpt2::payload::transaction::TransactionPayload) -> Bson {
    use crate::cpt2::payload::transaction::TransactionPayload;
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

fn milestone_payload_to_bson(payload: &crate::cpt2::payload::milestone::MilestonePayload) -> Bson {
    use crate::cpt2::payload::milestone::MilestonePayload;
    let mut doc = Document::new();
    doc.insert("kind", MilestonePayload::KIND as i32);
    doc.insert("milestone_id", payload.id().to_string());
    doc.insert("essence", milestone_essence_to_bson(&payload.essence()));
    doc.insert(
        "signatures",
        payload.signatures().iter().map(|s| hex::encode(s)).collect::<Vec<_>>(),
    );
    Bson::Document(doc)
}

fn indexation_payload_to_bson(payload: &crate::cpt2::payload::indexation::IndexationPayload) -> Bson {
    use crate::cpt2::payload::indexation::IndexationPayload;
    let mut doc = Document::new();
    doc.insert("kind", IndexationPayload::KIND as i32);
    doc.insert("index", hex::encode(payload.index()));
    doc.insert("data", hex::encode(payload.data()));
    Bson::Document(doc)
}

fn receipt_payload_to_bson(payload: &crate::cpt2::payload::receipt::ReceiptPayload) -> Bson {
    use crate::cpt2::{
        output::SignatureLockedSingleOutput,
        payload::receipt::ReceiptPayload,
    };
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
                doc.insert(
                    "output",
                    doc! {
                        "kind": SignatureLockedSingleOutput::KIND as i32,
                        "address": address_to_bson(f.output().address()),
                        "amount": f.output().amount() as i64,
                    },
                );
                doc
            })
            .collect::<Vec<_>>(),
    );
    doc.insert("transaction", payload_to_bson(payload.transaction()));
    doc.insert("amount", payload.amount() as i64);
    Bson::Document(doc)
}

fn treasury_payload_to_bson(payload: &crate::cpt2::payload::treasury::TreasuryTransactionPayload) -> Bson {
    use crate::cpt2::payload::treasury::TreasuryTransactionPayload;
    let mut doc = Document::new();
    doc.insert("kind", TreasuryTransactionPayload::KIND as i32);
    doc.insert("input", input_to_bson(payload.input()));
    doc.insert("output", output_to_bson(payload.output()));
    Bson::Document(doc)
}

fn transaction_essence_to_bson(essence: &crate::cpt2::payload::transaction::Essence) -> Bson {
    use crate::cpt2::payload::transaction::Essence;
    let mut doc = Document::new();
    match essence {
        Essence::Regular(r) => {
            doc.insert(
                "inputs",
                r.inputs().iter().map(|i| input_to_bson(i)).collect::<Vec<_>>(),
            );
            doc.insert(
                "outputs",
                r.outputs().iter().map(|o| output_to_bson(o)).collect::<Vec<_>>(),
            );
            doc.insert("payload", r.payload().as_ref().map(|p| payload_to_bson(p)));
        }
    }
    Bson::Document(doc)
}

fn milestone_essence_to_bson(essence: &crate::cpt2::payload::milestone::MilestonePayloadEssence) -> Bson {
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

fn unlock_block_to_bson(unlock_block: &crate::cpt2::unlock::UnlockBlock) -> Bson {
    use crate::cpt2::{
        signature::SignatureUnlock,
        unlock::{
            ReferenceUnlock,
            UnlockBlock,
        },
    };
    let mut doc = Document::new();
    match unlock_block {
        UnlockBlock::Signature(s) => match s {
            SignatureUnlock::Ed25519(s) => {
                doc.insert("kind", SignatureUnlock::KIND as i32);
                doc.insert("public_key", hex::encode(s.public_key()));
                doc.insert("signature", hex::encode(s.signature()));
            }
        },
        UnlockBlock::Reference(r) => {
            doc.insert("kind", ReferenceUnlock::KIND as i32);
            doc.insert("index", r.index() as i32);
        }
    }
    Bson::Document(doc)
}

fn input_to_bson(input: &crate::cpt2::input::Input) -> Bson {
    use crate::cpt2::input::{
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

fn output_to_bson(output: &crate::cpt2::output::Output) -> Bson {
    use crate::cpt2::output::{
        Output,
        SignatureLockedDustAllowanceOutput,
        SignatureLockedSingleOutput,
        TreasuryOutput,
    };
    let mut doc = Document::new();
    match output {
        Output::SignatureLockedSingle(s) => {
            doc.insert("kind", SignatureLockedSingleOutput::KIND as i32);
            doc.insert("address", address_to_bson(s.address()));
            doc.insert("amount", s.amount() as i64);
        }
        Output::SignatureLockedDustAllowance(d) => {
            doc.insert("kind", SignatureLockedDustAllowanceOutput::KIND as i32);
            doc.insert("address", address_to_bson(d.address()));
            doc.insert("amount", d.amount() as i64);
        }
        Output::Treasury(t) => {
            doc.insert("kind", TreasuryOutput::KIND as i32);
            doc.insert("amount", t.amount() as i64);
        }
    }
    Bson::Document(doc)
}

fn address_to_bson(address: &crate::cpt2::address::Address) -> Bson {
    use crate::cpt2::address::{
        Address,
        Ed25519Address,
    };
    let mut doc = Document::new();
    match address {
        Address::Ed25519(a) => {
            doc.insert("kind", Ed25519Address::KIND as i32);
            doc.insert("data", a.to_string());
        }
    }
    Bson::Document(doc)
}
