// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
// use chronicle_api::application::*;
use chronicle_broker::application::{
    null::NullConfig,
    ChronicleBroker,
};
use chronicle_common::{
    alert,
    config::AlertConfig,
};
use serde::{
    Deserialize,
    Serialize,
};

const TOKIO_THREAD_STACK_SIZE: usize = 4 * 4 * 1024 * 1024;

/// Chronicle system struct
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone)]
pub struct Chronicle {
    /// Permanode application
    #[cfg(all(feature = "null"))]
    broker: ChronicleBroker<NullConfig>,
    // // The Api application
    // api: ChronicleAPI,
    /// Alert config
    alert: AlertConfig,
}

/// Chronicle event type
pub enum ChronicleEvent {
    // Get up to date -api copy
    // Api(Event<ChronicleAPI>),
    /// Get up to date -broker copy
    #[cfg(all(feature = "null"))]
    Broker(Event<ChronicleBroker<NullConfig>>),
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

#[cfg(all(feature = "null"))]
impl From<Event<ChronicleBroker<NullConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<NullConfig>>) -> Self {
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
        #[cfg(any(feature = "null"))]
        {
            let broker = self.broker.clone();
            let broker_scope_id = rt.start("broker".to_string(), broker).await?.scope_id();
            if let Some(broker) = rt.subscribe(broker_scope_id, "broker".to_string()).await? {
                if self.broker != broker {
                    self.broker = broker;
                    rt.publish(self.broker.clone()).await;
                }
            }
        }
        log::info!("Chronicle Started Broker");
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult<Self::Data> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                #[cfg(any(feature = "null"))]
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
    runtime.block_on(chronicle());
}

async fn chronicle() {
    let backserver_addr: std::net::SocketAddr = std::env::var("BACKSERVER_ADDR").map_or_else(
        |_| ([127, 0, 0, 1], 9999).into(),
        |n| {
            n.parse()
                .expect("Invalid BACKSERVER_ADDR env, use this format '127.0.0.1:9999' ")
        },
    );
    Runtime::from_config::<Chronicle>()
        .await
        .expect("Failed to run chronicle system")
        .backserver(backserver_addr)
        .await
        .expect("Failed to run backserver")
        .block_on()
        .await
        .expect("Failed to shutdown the runtime gracefully");
}
