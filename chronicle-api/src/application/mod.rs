// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::websocket::Websocket;
use chronicle_common::get_config_async;

#[cfg(feature = "rocket_listener")]
use crate::listener::RocketListener;

/// The Chronicle API. Defines endpoints which can be used to
/// retrieve data from the scylla database.
pub struct ChronicleAPI;

/// A Chronicle API Event
pub enum ChronicleAPIEvent {}

#[async_trait]
impl Actor for ChronicleAPI {
    type Dependencies = ();
    type Event = ChronicleAPIEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<std::marker::PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Initializing).await.ok();
        #[cfg(feature = "rocket_listener")]
        rt.spawn_actor_unsupervised(super::listener::ListenerBuilder::new().data(RocketListener).build())
            .await?;

        rt.spawn_actor_unsupervised(Websocket {
            listen_address: get_config_async().await.broker_config.websocket_address,
        })
        .await?;
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        _: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<std::marker::PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(_) = rt.next_event().await {}
        Ok(())
    }
}
