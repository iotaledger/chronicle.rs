// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use chronicle_common::mongodb::{
    options::ClientOptions,
    Client,
};

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct ChronicleAPI {
    #[cfg(feature = "mongo_api")]
    mongo_config: chronicle_common::config::mongo::MongoConfig,
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
        log::info!("{:?} is initializing", &rt.service().directory());
        register_metrics();
        let client_opts: ClientOptions = self.mongo_config.clone().into();
        let client = Client::with_options(client_opts).map_err(|e| anyhow::anyhow!(e))?;
        let rocket = backstage::prefab::rocket::RocketServer::new(
            super::listener::construct_rocket(client.database("permanode"))
                .ignite()
                .await
                .map_err(|e| anyhow::anyhow!(e))?,
        );
        rt.spawn("rocket".to_string(), rocket).await?;
        Ok(())
    }

    async fn run(&mut self, rt: &mut Rt<Self, Sup>, _data: Self::Data) -> ActorResult<()> {
        log::info!("{:?} is running", &rt.service().directory());
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
        log::info!("{:?} exited its event loop", &rt.service().directory());
        Ok(())
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
