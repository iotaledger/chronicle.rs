// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::config::Config;
use std::collections::HashSet;

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
pub struct ChronicleAPI;

/// A Chronicle API Event
pub enum ChronicleAPIEvent {
    Report(ScopeId, Service),
}

impl<T> ReportEvent<T> for ChronicleAPIEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::Report(scope_id, service)
    }
}

impl<T> EolEvent<T> for ChronicleAPIEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _: T, r: ActorResult<()>) -> Self {
        Self::Report(scope_id, service)
    }
}

#[async_trait]
impl<Sup: SupHandle<Self>> Actor<Sup> for ChronicleAPI {
    type Data = ();
    type Channel = AbortableUnboundedChannel<ChronicleAPIEvent>;

    async fn init(&mut self, rt: &mut Rt<Self, Sup>) -> ActorResult<Self::Data> {
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
        rt.spawn("rocket".to_string(), rocket).await?;
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, Sup>, data: Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ChronicleAPIEvent::Report(scope_id, service) => {
                    rt.upsert_microservice(scope_id, service);
                    if rt.microservices_stopped() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
