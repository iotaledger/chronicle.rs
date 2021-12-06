// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
use chronicle_api::application::*;
use chronicle_broker::application::*;
use chronicle_common::{
    alert,
    config::AlertConfig,
    metrics::*,
};
use scylla_rs::prelude::*;
use serde::{
    Deserialize,
    Serialize,
};

const TOKIO_THREAD_STACK_SIZE: usize = 4 * 4 * 1024 * 1024;

/// Chronicle system struct
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct Chronicle {
    /// Scylla application
    scylla: Scylla,
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    /// Permanode application
    broker: ChronicleBroker<chronicle_filter::PermanodeConfig>,
    /// Selective Permanode application
    #[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
    broker: ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>,
    /// The Api application
    api: ChronicleAPI,
    /// Alert config
    alert: AlertConfig,
}

/// Chronicle event type
pub enum ChronicleEvent {
    /// Get up to date scylla config copy
    Scylla(Event<Scylla>),
    /// Get up to date -api copy
    Api(Event<ChronicleAPI>),
    /// Get up to date -broker copy
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    Broker(Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>),
    #[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
    Broker(Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>),
    /// Report and Eol variant used by children
    MicroService(ScopeId, Service, Option<ActorResult<()>>),
    /// Shutdown chronicle variant
    Shutdown,
}

impl ShutdownEvent for ChronicleEvent {
    fn shutdown_event() -> Self {
        Self::Shutdown
    }
}

impl<T> ReportEvent<T> for ChronicleEvent {
    fn report_event(scope_id: ScopeId, service: Service) -> Self {
        Self::MicroService(scope_id, service, None)
    }
}

impl<T> EolEvent<T> for ChronicleEvent {
    fn eol_event(scope_id: ScopeId, service: Service, _actor: T, r: ActorResult<()>) -> Self {
        Self::MicroService(scope_id, service, Some(r))
    }
}

impl From<Event<Scylla>> for ChronicleEvent {
    fn from(e: Event<Scylla>) -> Self {
        Self::Scylla(e)
    }
}
impl From<Event<ChronicleAPI>> for ChronicleEvent {
    fn from(e: Event<ChronicleAPI>) -> Self {
        Self::Api(e)
    }
}

#[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
impl From<Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<chronicle_filter::PermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

#[cfg(all(feature = "selective-permanode", not(feature = "permanode")))]
impl From<Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<chronicle_filter::SelectivePermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

/// Chronicle system actor implementation
#[async_trait]
impl<S> Actor<S> for Chronicle
where
    S: SupHandle<Self>,
{
    type Data = ();
    type Channel = UnboundedChannel<ChronicleEvent>;
    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        // init the alert
        alert::init(self.alert.clone());
        //
        // - Scylla
        let scylla_scope_id = rt.start("scylla".to_string(), self.scylla.clone()).await?.scope_id();
        log::info!("Chronicle Started Scylla");
        if let Some(scylla) = rt.subscribe::<Scylla>(scylla_scope_id, "scylla".to_string()).await? {
            if self.scylla != scylla {
                self.scylla = scylla;
                log::info!("Chronicle published new Scylla");
                rt.publish(self.scylla.clone()).await;
            }
        }
        log::info!("Chronicle subscribed to Scylla");

        //
        // - brokern
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        let broker = self.broker.clone();
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        let broker_scope_id = rt.start("broker".to_string(), broker).await?.scope_id();
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
        {
            if let Some(broker) = rt.subscribe(broker_scope_id, "broker".to_string()).await? {
                if self.broker != broker {
                    self.broker = broker;
                    rt.publish(self.broker.clone()).await;
                }
            }
        }
        log::info!("Chronicle Started Broker");
        //
        // - api
        let api_scope_id = rt.start("api".to_string(), self.api.clone()).await?.scope_id();
        if let Some(api) = rt.subscribe::<ChronicleAPI>(api_scope_id, "api".to_string()).await? {
            if self.api != api {
                self.api = api;
                rt.publish(self.api.clone()).await;
            }
        }
        log::info!("Chronicle Started Api");
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<Self::Data> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
                ChronicleEvent::Broker(broker_event) => {
                    if let Event::Published(_, _, broker) = broker_event {
                        self.broker = broker;
                        rt.publish(self.broker.clone()).await;
                    }
                }
                ChronicleEvent::Scylla(scylla_event) => {
                    if let Event::Published(_, _, scylla) = scylla_event {
                        self.scylla = scylla;
                        rt.publish(self.scylla.clone()).await;
                    }
                }
                ChronicleEvent::Api(api_event) => {
                    if let Event::Published(_, _, api) = api_event {
                        self.api = api;
                        rt.publish(self.api.clone()).await;
                    }
                }
                ChronicleEvent::MicroService(scope_id, service, _result_opt) => {
                    rt.upsert_microservice(scope_id, service);
                    if rt.microservices_stopped() {
                        break;
                    }
                }
                ChronicleEvent::Shutdown => rt.inbox_mut().close(),
            }
        }
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
        .expect("Expected to build tokio runtime");
    runtime.block_on(chronicle());
}

async fn chronicle() {
    Runtime::from_config::<Chronicle>()
        .await
        .expect("Chronicle Runtime to run")
        .block_on()
        .await
        .expect("Runtime to shutdown gracefully");
}
