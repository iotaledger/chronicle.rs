// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::config::PartitionConfig;
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    hash::{
        Hash,
        Hasher,
    },
    iter::FromIterator,
};

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct ChronicleAPI {
    keyspaces: HashSet<KeyspacePartitionConfig>,
}

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
        register_metrics();
        let keyspaces: HashMap<String, PartitionConfig> = self.keyspaces.clone().into_iter().collect();
        let rocket = backstage::prefab::rocket::RocketServer::new(
            super::listener::construct_rocket()
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct KeyspacePartitionConfig {
    name: KeyspaceName,
    partition_config: PartitionConfig,
}

impl Hash for KeyspacePartitionConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl FromIterator<KeyspacePartitionConfig> for HashMap<String, PartitionConfig> {
    fn from_iter<I: IntoIterator<Item = KeyspacePartitionConfig>>(iter: I) -> Self {
        let mut c = HashMap::new();

        for i in iter {
            c.insert(i.name, i.partition_config);
        }

        c
    }
}

/// metrics

fn register_metrics() {
    use chronicle_common::metrics::*;
    REGISTRY
        .register(Box::new(INCOMING_REQUESTS.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_CODE_COLLECTOR.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(RESPONSE_TIME_COLLECTOR.clone()))
        .expect("Could not register collector");

    REGISTRY
        .register(Box::new(CONFIRMATION_TIME_COLLECTOR.clone()))
        .expect("Could not register collector");
}
