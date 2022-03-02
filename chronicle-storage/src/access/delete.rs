// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::*;

/// Delete record from legacy_outputs table
impl Delete<Bee<OutputId>, (), LegacyOutputRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> DeleteStatement {
        parse_statement!(
            "DELETE FROM #.legacy_outputs
            WHERE output_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, output_id: &Bee<OutputId>, _: &()) -> B {
        builder.value(output_id)
    }
}

/// Delete record from tags table
impl Delete<(String, MsRangeId), (Bee<MilestoneIndex>, Bee<MessageId>), TagRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> DeleteStatement {
        parse_statement!(
            "DELETE FROM #.tags
            WHERE tag = ?
            AND ms_range_id = ?
            AND milestone_index = ?
            AND message_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(
        builder: B,
        (index, ms_range_id): &(String, MsRangeId),
        (milestone_index, message_id): &(Bee<MilestoneIndex>, Bee<MessageId>),
    ) -> B {
        builder
            .value(index)
            .value(ms_range_id)
            .value(milestone_index)
            .value(message_id)
    }
}

/// Delete record from parents table
impl Delete<Bee<MessageId>, Bee<MessageId>, ParentRecord> for ChronicleKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> DeleteStatement {
        parse_statement!(
            "DELETE FROM #.parents
            WHERE parent_id = ?
            AND message_id = ?",
            self.name()
        )
    }
    fn bind_values<B: Binder>(builder: B, parent_id: &Bee<MessageId>, message_id: &Bee<MessageId>) -> B {
        builder.value(parent_id).value(message_id)
    }
}
