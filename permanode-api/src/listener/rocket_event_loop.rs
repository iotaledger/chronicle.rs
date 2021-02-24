use super::*;
use bee_rest_api::handlers::{
    message::MessageResponse,
    message_children::MessageChildrenResponse,
    message_metadata::{
        LedgerInclusionStateDto,
        MessageMetadataResponse,
    },
    messages_find::MessagesForIndexResponse,
    milestone::MilestoneResponse,
    output::OutputResponse,
    outputs_ed25519::OutputsForAddressResponse,
};
use mpsc::unbounded_channel;
use permanode_storage::{
    access::{
        Bee,
        CreatedOutput,
        Ed25519Address,
        GetSelectRequest,
        HashedIndex,
        IndexMessages,
        MessageChildren,
        MessageId,
        MessageRow,
        MilestoneIndex,
        OutputId,
        OutputIds,
        Outputs,
        Payload,
        SelectRequest,
        SingleMilestone,
        TransactionData,
        HASHED_INDEX_LENGTH,
    },
    keyspaces::Mainnet,
};
use rocket::get;
use rocket_contrib::json::Json;
use scylla_cql::TryInto;
use std::{
    borrow::Cow,
    ops::Deref,
    str::FromStr,
};
use tokio::sync::mpsc;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<PermanodeSender<H>> for Listener<RocketListener> {
    async fn event_loop(
        &mut self,
        _status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Running);
        if let Some(ref mut supervisor) = supervisor {
            supervisor
                .send(PermanodeEvent::Children(PermanodeChild::Listener(self.service.clone())))
                .map_err(|_| Need::Abort)?;
        }
        rocket::ignite()
            .mount(
                "/",
                routes![
                    get_message,
                    get_message_metadata,
                    get_message_children,
                    get_message_by_index,
                    get_output,
                    get_ed25519_outputs,
                    get_milestone
                ],
            )
            .launch()
            .await
            .map_err(|_| Need::Abort)
    }
}

async fn query<'a, V, S: Select<'a, K, V>, K>(request: SelectRequest<'a, S, K, V>) -> Result<V, Cow<'static, str>> {
    let (sender, mut inbox) = unbounded_channel::<Event>();
    let worker = Box::new(DecoderWorker(sender));

    let decoder = request.send_local(worker);

    while let Some(event) = inbox.recv().await {
        match event {
            Event::Response { giveload } => {
                let res = decoder.decode(giveload);
                if let Ok(Some(message)) = res {
                    return Ok(message);
                }
            }
            Event::Error { kind } => return Err(kind.to_string().into()),
        }
    }

    Err("Failed to receive response!".into())
}

#[get("/messages/<message_id>")]
async fn get_message(message_id: String) -> Result<Json<MessageResponse>, Cow<'static, str>> {
    let request = Mainnet.select::<Bee<Message>>(&MessageId::from_str(&message_id).unwrap().into());
    query(request)
        .await?
        .deref()
        .try_into()
        .map(|dto| Json(MessageResponse(dto)))
        .map_err(|e| e.into())
}

#[get("/messages/<message_id>/metadata")]
async fn get_message_metadata(message_id: String) -> Result<Json<MessageMetadataResponse>, Cow<'static, str>> {
    let request = Mainnet.select::<MessageRow>(&MessageId::from_str(&message_id).unwrap().into());
    message_row_to_response(query(request).await?).map(|res| Json(res))
}

#[get("/messages/<message_id>/children")]
async fn get_message_children(message_id: String) -> Result<Json<MessageChildrenResponse>, Cow<'static, str>> {
    let request = Mainnet.select::<MessageChildren>(&MessageId::from_str(&message_id).unwrap().into());
    // TODO: Paging
    let children = query(request).await?;
    let count = children.rows_count();
    let max_results = 1000;

    Ok(Json(MessageChildrenResponse {
        message_id,
        max_results,
        count,
        children_message_ids: children.take(max_results).map(|id| id.to_string()).collect(),
    }))
}

#[get("/messages?<index>")]
async fn get_message_by_index(index: String) -> Result<Json<MessagesForIndexResponse>, Cow<'static, str>> {
    let bytes: [u8; HASHED_INDEX_LENGTH] = hex::decode(index.clone())
        .map_err(|_| Cow::from("Invalid Hex character in index!"))?
        .try_into()
        .map_err(|_| Cow::from("Invalid index length!"))?;

    let request = Mainnet.select::<IndexMessages>(&HashedIndex::from(bytes).into());
    // TODO: Paging
    let messages = query(request).await?;
    let count = messages.rows_count();
    let max_results = 1000;

    Ok(Json(MessagesForIndexResponse {
        index,
        max_results,
        count,
        message_ids: messages.take(max_results).map(|id| id.to_string()).collect(),
    }))
}

#[get("/outputs/<output_id>")]
async fn get_output(output_id: String) -> Result<Json<OutputResponse>, Cow<'static, str>> {
    let output_id = OutputId::from_str(&output_id).unwrap();
    let request = Mainnet.select::<Outputs>(&output_id.into());
    let outputs = query(request).await?;
    let (output, is_spent) = {
        let mut output = None;
        let mut is_spent = false;
        for row in outputs {
            match row.data {
                TransactionData::Input(_) => {}
                TransactionData::Output(o) => {
                    output = Some(CreatedOutput::new(row.message_id.into_inner(), o));
                }
                TransactionData::Unlock(_) => {
                    is_spent = true;
                }
            }
        }
        (
            output.ok_or(Cow::from(format!("No output found for id {}", output_id)))?,
            is_spent,
        )
    };
    Ok(Json(OutputResponse {
        message_id: output.message_id().to_string(),
        transaction_id: output_id.transaction_id().to_string(),
        output_index: output_id.index(),
        is_spent,
        output: output.inner().try_into().map_err(|e| Cow::from(e))?,
    }))
}

#[get("/addresses/ed25519/<address>/outputs")]
async fn get_ed25519_outputs(address: String) -> Result<Json<OutputsForAddressResponse>, Cow<'static, str>> {
    let request = Mainnet.select::<OutputIds>(&Ed25519Address::from_str(&address).unwrap().into());

    // TODO: Paging
    let outputs = query(request).await?;
    let count = outputs.rows_count();
    let max_results = 1000;

    Ok(Json(OutputsForAddressResponse {
        address_type: 1,
        address,
        max_results,
        count,
        output_ids: outputs.take(max_results).map(|id| id.to_string()).collect(),
    }))
}

#[get("/milestones/<index>")]
async fn get_milestone(index: u32) -> Result<Json<MilestoneResponse>, Cow<'static, str>> {
    let request = Mainnet.select::<SingleMilestone>(&MilestoneIndex::from(index).into());
    query(request)
        .await?
        .get()
        .map(|milestone| {
            Json(MilestoneResponse {
                milestone_index: index,
                message_id: milestone.message_id().to_string(),
                timestamp: milestone.timestamp(),
            })
        })
        .ok_or(Cow::from(format!("No milestone found for index {}", index)))
}

fn message_row_to_response(
    MessageRow {
        id: message_id,
        message,
        metadata,
    }: MessageRow,
) -> Result<MessageMetadataResponse, Cow<'static, str>> {
    // TODO: let ymrsi_delta = 8;
    // TODO: let omrsi_delta = 13;
    // TODO: let below_max_depth = 15;
    let is_solid;
    let referenced_by_milestone_index;
    let milestone_index;
    let ledger_inclusion_state;
    let conflict_reason;
    // TODO: Remove these when we get the solid milestone
    let should_promote = None;
    let should_reattach = None;
    let metadata = metadata.ok_or("No metadata provided!")?;
    let message = message.ok_or("No message provided!")?;

    if let Some(milestone) = metadata.milestone_index() {
        // message is referenced by a milestone
        is_solid = true;
        referenced_by_milestone_index = Some(*milestone);

        if metadata.flags().is_milestone() {
            milestone_index = Some(*milestone);
        } else {
            milestone_index = None;
        }

        ledger_inclusion_state = Some(if let Some(Payload::Transaction(_)) = message.payload() {
            if metadata.conflict() != 0 {
                conflict_reason = Some(metadata.conflict());
                LedgerInclusionStateDto::Conflicting
            } else {
                conflict_reason = None;
                // maybe not checked by the ledger yet, but still
                // returning "included". should
                // `metadata.flags().is_conflicting` return an Option
                // instead?
                LedgerInclusionStateDto::Included
            }
        } else {
            conflict_reason = None;
            LedgerInclusionStateDto::NoTransaction
        });
        //TODO: should_reattach = None;
        //TODO: should_promote = None;
    } else if metadata.flags().is_solid() {
        // message is not referenced by a milestone but solid
        is_solid = true;
        referenced_by_milestone_index = None;
        milestone_index = None;
        ledger_inclusion_state = None;
        conflict_reason = None;

        /* TODO:
        let lmi = *tangle.get_solid_milestone_index();
        // unwrap() of OMRSI/YMRSI is safe since message is solid
        if (lmi - *metadata.omrsi().unwrap().index()) > below_max_depth {
            should_promote = Some(false);
            should_reattach = Some(true);
        } else if (lmi - *metadata.ymrsi().unwrap().index()) > ymrsi_delta
            || (lmi - omrsi_delta) > omrsi_delta
        {
            should_promote = Some(true);
            should_reattach = Some(false);
        } else {
            should_promote = Some(false);
            should_reattach = Some(false);
        };
        */
    } else {
        // the message is not referenced by a milestone and not solid
        is_solid = false;
        referenced_by_milestone_index = None;
        milestone_index = None;
        ledger_inclusion_state = None;
        conflict_reason = None;
        //TODO: should_reattach = Some(true);
        //TODO: should_promote = Some(false);
    }

    Ok(MessageMetadataResponse {
        message_id: message_id.to_string(),
        parent_message_ids: message.parents().iter().map(|id| id.to_string()).collect(),
        is_solid,
        referenced_by_milestone_index,
        milestone_index,
        ledger_inclusion_state,
        conflict_reason,
        should_promote,
        should_reattach,
    })
}
