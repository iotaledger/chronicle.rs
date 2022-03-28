// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
use chronicle_api::application::*;

#[cfg(not(any(feature = "mongo",)))]
use chronicle_broker::application::null::NullConfig;
#[cfg(feature = "mongo")]
use permanode_mongo::PermanodeMongoConfig;

use chronicle_broker::{
    application::ChronicleBroker,
    filter::FilterBuilder,
};

use chronicle_common::{
    alert,
    config::AlertConfig,
};
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};

const TOKIO_THREAD_STACK_SIZE: usize = 4 * 4 * 1024 * 1024;

/// Chronicle system struct
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct Chronicle<B: FilterBuilder> {
    /// Broker application
    broker: ChronicleBroker<B>,
    /// The Api application
    api: ChronicleAPI,
    /// Alert config
    alert: AlertConfig,
}

/// Chronicle event type
pub enum ChronicleEvent<B: FilterBuilder> {
    /// Get up to date -api copy
    Api(Event<ChronicleAPI>),
    /// Get up to date -broker copy
    Broker(Event<ChronicleBroker<B>>),
    /// Report and Eol variant used by children
    MicroService(ScopeId, Service, Option<ActorResult<()>>),
    /// Shutdown chronicle variant
    Shutdown,
}

impl<B: FilterBuilder> ShutdownEvent for ChronicleEvent<B> {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

impl<T, B: FilterBuilder> ReportEvent<T> for ChronicleEvent<B> {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::MicroService(scope_id, service, None)
    }
}

impl<T, B: FilterBuilder> EolEvent<T> for ChronicleEvent<B> {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, r: ActorResult<()>) -> Self {
        Self::MicroService(scope_id, service, Some(r))
    }
}

impl<B: FilterBuilder> From<Event<ChronicleBroker<B>>> for ChronicleEvent<B> {
    fn from(e: Event<ChronicleBroker<B>>) -> Self {
        Self::Broker(e)
    }
}

impl<B: FilterBuilder> From<Event<ChronicleAPI>> for ChronicleEvent<B> {
    fn from(e: Event<ChronicleAPI>) -> Self {
        Self::Api(e)
    }
}
/// Chronicle system actor implementation
#[async_trait]
impl<S, B: FilterBuilder> Actor<S> for Chronicle<B>
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<ChronicleEvent<B>>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // init the alert
        alert::init(self.alert.clone());
        let broker = self.broker.clone();
        let broker_scope_id = rt.start("broker".to_string(), broker).await?.scope_id();
        if let Some(broker) = rt.subscribe(broker_scope_id, "broker".to_string()).await? {
            if self.broker != broker {
                self.broker = broker;
                rt.publish(self.broker.clone()).await;
            }
        }
        let api = self.api.clone();
        let api_scope_id = rt.start("api".to_string(), api).await?.scope_id();
        if let Some(api) = rt.subscribe(api_scope_id, "api".to_string()).await? {
            if self.api != api {
                self.api = api;
                rt.publish(self.api.clone()).await;
            }
        }

        log::info!("Chronicle Started Broker");
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<Self::Data> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                ChronicleEvent::Api(api_event) => {
                    if let Event::Published(_, _, api) = api_event {
                        self.api = api;
                        rt.publish(self.api.clone()).await;
                    }
                }
                #[cfg(any(feature = "null", feature = "mongo"))]
                ChronicleEvent::Broker(broker_event) => {
                    if let Event::Published(_, _, broker) = broker_event {
                        self.broker = broker;
                        rt.publish(self.broker.clone()).await;
                    }
                }
                ChronicleEvent::MicroService(scope_id, service, _result_opt) => {
                    let is_stopped = service.is_stopped();
                    rt.upsert_microservice(scope_id, service);
                    if !rt.service().is_stopping() {
                        if is_stopped {
                            rt.stop().await;
                        } else {
                            let same_status = rt.service().status().clone();
                            rt.update_status(same_status).await;
                        }
                    } else {
                        rt.update_status(ServiceStatus::Stopping).await;
                    }
                    if rt.microservices_stopped() {
                        rt.inbox_mut().close();
                    }
                }
                ChronicleEvent::Shutdown => rt.stop().await,
            }
        }
        log::info!("Chronicle exited its event loop");
        Ok(())
    }
}

fn main() {
    dotenv::dotenv().ok();
    #[cfg(not(feature = "console"))]
    {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::Builder::from_env(env).init();
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
        .build()
        .expect("Failed to build tokio runtime");
    #[cfg(not(any(feature = "mongo",)))]
    runtime.block_on(chronicle::<NullConfig>());
    #[cfg(feature = "mongo")]
    runtime.block_on(chronicle::<PermanodeMongoConfig>());
}

async fn chronicle<B: FilterBuilder + DeserializeOwned>() {
    let backserver_addr: std::net::SocketAddr = std::env::var("BACKSERVER_ADDR").map_or_else(
        |_| ([127, 0, 0, 1], 9999).into(),
        |n| {
            n.parse()
                .expect("Invalid BACKSERVER_ADDR env, use this format '127.0.0.1:9999' ")
        },
    );
    Runtime::from_config::<Chronicle<B>>()
        .await
        .expect("Failed to run chronicle system")
        .backserver(backserver_addr)
        .await
        .expect("Failed to run backserver")
        .block_on()
        .await
        .expect("Failed to shutdown the runtime gracefully");
}
