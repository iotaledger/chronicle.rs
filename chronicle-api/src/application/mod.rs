// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::config::Config;
use std::collections::HashSet;

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
pub struct ChronicleAPI;

/// A Chronicle API Event
#[supervise]
pub enum ChronicleAPIEvent {
    #[report]
    Report(ScopeId, Service),
    #[shutdown]
    Shutdown,
}

#[async_trait]
impl<Sup: Send + SupHandle<Self>> Actor<Sup> for ChronicleAPI {
    type Data = ();
    type Channel = AbortableUnboundedChannel<ChronicleAPIEvent>;

    async fn init(&mut self, rt: &mut Rt<Self, Sup>) -> ActorResult<Self::Data> {
        rt.update_status(ServiceStatus::Initializing).await;

        let storage_config = rt
            .lookup::<Config>(rt.parent_id().ok_or_else(|| anyhow::anyhow!("No parent id!"))?)
            .await
            .ok_or_else(|| anyhow::anyhow!("No config found!"))?
            .storage_config;

        let keyspaces = storage_config
            .keyspaces
            .iter()
            .cloned()
            .map(|k| k.name)
            .collect::<HashSet<_>>();

        let rocket = backstage::prefab::rocket::RocketServer::new(
            super::listener::construct_rocket()
                .manage(storage_config.partition_config.clone())
                .manage(keyspaces)
                .ignite()
                .await
                .map_err(|e| anyhow::anyhow!(e))?,
        );
        rt.spawn(Some("rocket".to_string()), rocket).await?;
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, Sup>, data: Self::Data) -> ActorResult<()> {
        rt.update_status(ServiceStatus::Running).await;
        while let Some(_) = rt.inbox_mut().next().await {}
        Ok(())
    }
}
