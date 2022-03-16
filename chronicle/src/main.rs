// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#![warn(missing_docs)]
//! # Chronicle
use async_trait::async_trait;
use backstage::core::*;
use chronicle_api::application::*;
use chronicle_broker::application::{
    permanode::{
        PermanodeConfig,
        SelectivePermanodeConfig,
    },
    ChronicleBroker,
};
use chronicle_common::{
    alert,
    config::AlertConfig,
};
use chronicle_storage::{
    mongodb::{
        bson::doc,
        options::ClientOptions,
        Client,
        IndexModel,
    },
    MessageRecord,
};
use serde::{
    Deserialize,
    Serialize,
};

const TOKIO_THREAD_STACK_SIZE: usize = 4 * 4 * 1024 * 1024;

/// Chronicle system struct
#[derive(Debug, Deserialize, PartialEq, Default, Clone)]
pub struct Chronicle {
    /// Permanode application
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    broker: ChronicleBroker<PermanodeConfig>,
    /// Selective Permanode application
    #[cfg(feature = "selective-permanode")]
    broker: ChronicleBroker<SelectivePermanodeConfig>,
    /// The Api application
    api: ChronicleAPI,
    /// Alert config
    alert: AlertConfig,
    // Database config
    client_opts: ClientOptions,
}

/// Chronicle event type
pub enum ChronicleEvent {
    /// Get up to date -api copy
    Api(Event<ChronicleAPI>),
    /// Get up to date -broker copy
    #[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
    Broker(Event<ChronicleBroker<PermanodeConfig>>),
    #[cfg(feature = "selective-permanode")]
    Broker(Event<ChronicleBroker<SelectivePermanodeConfig>>),
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

impl From<Event<ChronicleAPI>> for ChronicleEvent {
    fn from(e: Event<ChronicleAPI>) -> Self {
        Self::Api(e)
    }
}

#[cfg(all(feature = "permanode", not(feature = "selective-permanode")))]
impl From<Event<ChronicleBroker<PermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<PermanodeConfig>>) -> Self {
        Self::Broker(e)
    }
}

#[cfg(feature = "selective-permanode")]
impl From<Event<ChronicleBroker<SelectivePermanodeConfig>>> for ChronicleEvent {
    fn from(e: Event<ChronicleBroker<SelectivePermanodeConfig>>) -> Self {
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
        let client = Client::with_options(self.client_opts.clone())
            .map_err(|e| ActorError::exit(anyhow::anyhow!("No database connection!")))?;
        init_database(client).await.map_err(|e| {
            log::error!("{}", e);
            e
        })?;

        // - api
        let api_scope_id = rt.start("api".to_string(), self.api.clone()).await?.scope_id();
        if let Some(api) = rt.subscribe::<ChronicleAPI>(api_scope_id, "api".to_string()).await? {
            if self.api != api {
                self.api = api;
                rt.publish(self.api.clone()).await;
            }
        }
        log::info!("Chronicle Started Api");
        #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
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
                #[cfg(any(feature = "permanode", feature = "selective-permanode"))]
                ChronicleEvent::Broker(broker_event) => {
                    if let Event::Published(_, _, broker) = broker_event {
                        self.broker = broker;
                        rt.publish(self.broker.clone()).await;
                    }
                }
                ChronicleEvent::Api(api_event) => {
                    if let Event::Published(_, _, api) = api_event {
                        self.api = api;
                        rt.publish(self.api.clone()).await;
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
        .expect("Expected to build tokio runtime");
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
    todo!("fix below")
    // Runtime::from_config::<Chronicle>()
    //     .await
    //     .expect("Failed to run chronicle system")
    //     .backserver(backserver_addr)
    //     .await
    //     .expect("Failed to run backserver")
    //     .block_on()
    //     .await
    //     .expect("Failed to shutdown the runtime gracefully");
}

async fn init_database(client: Client) -> anyhow::Result<()> {
    log::warn!("Initializing Chronicle data model",);
    let database = client.database("permanode");
    let message_records = database.collection::<MessageRecord>("messages");
    message_records
        .create_indexes(
            vec![
                doc! {"message_id_index": {"message_id": 1}},
                doc! {"transaction_id_index": {"message.payload.c_transaction_id": 1}},
                doc! {"outputs_by_address_index": {"message.payload.essence.outputs.data.address.data": 1}},
                // TODO: MOOOORE
            ]
            .into_iter()
            .map(|d| IndexModel::builder().keys(d).build()),
            None,
        )
        .await?;
    Ok(())
}
