use super::topics::WsTopic;
use serde::Serialize;
#[derive(Clone, Debug, Serialize)]
pub(crate) struct WsEvent {
    #[serde(rename = "type")]
    pub(crate) kind: WsTopic,
    #[serde(rename = "data")]
    pub(crate) inner: WsEventInner,
}

impl WsEvent {
    pub(crate) fn new(kind: WsTopic, inner: WsEventInner) -> Self {
        Self { kind, inner }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum WsEventInner {
    // SyncStatus(SyncStatusResponse),
    // MpsMetricsUpdated(MpsMetricsUpdatedResponse),
    // Milestone(MilestoneResponse),
    // SolidInfo(SolidInfoResponse),
    // ConfirmedInfo(ConfirmedInfoResponse),
    // ConfirmedMilestoneMetrics(ConfirmedMilestoneMetricsResponse),
    // MilestoneInfo(MilestoneInfoResponse),
    // Vertex(VertexResponse),
    // DatabaseSizeMetrics(DatabaseSizeMetricsResponse),
    // TipInfo(TipInfoResponse),
    PublicNodeStatus(PublicNodeStatusResponse),
    /* NodeStatus(NodeStatusResponse),
     * PeerMetric(PeersResponse), */
}

#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize)]
pub struct PublicNodeStatus {
    pub snapshot_index: u32,
    pub pruning_index: u32,
    pub is_healthy: bool,
    pub is_synced: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PublicNodeStatusResponse(pub PublicNodeStatus);

pub(crate) fn forward(event: PublicNodeStatus) -> WsEvent {
    event.into()
}

impl From<PublicNodeStatus> for WsEvent {
    fn from(val: PublicNodeStatus) -> Self {
        Self::new(WsTopic::PublicNodeStatus, WsEventInner::PublicNodeStatus(val.into()))
    }
}

impl From<PublicNodeStatus> for PublicNodeStatusResponse {
    fn from(val: PublicNodeStatus) -> Self {
        Self(val)
    }
}
