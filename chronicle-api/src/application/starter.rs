use super::*;
use crate::listener::ListenerBuilder;
#[cfg(feature = "rocket_listener")]
use crate::listener::RocketListener;
use anyhow::anyhow;
use chronicle_common::CONFIG;

#[async_trait]
impl<H> Starter<H> for ChronicleAPIBuilder<H>
where
    H: ChronicleAPIScope,
{
    type Ok = ChronicleAPISender<H>;

    type Error = anyhow::Error;

    type Input = ChronicleAPI<H>;

    async fn starter(mut self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        #[cfg(feature = "rocket_listener")]
        let rocket_listener = {
            let rocket = rocket::ignite();
            let rocket_listener_handle = rocket.shutdown();
            let rocket_listener = ListenerBuilder::new().data(RocketListener::new(rocket)).build();
            self = self.rocket_listener_handle(rocket_listener_handle);
            rocket_listener
        };

        // let websocket = WebsocketBuilder::new().build();
        // let (websocket_handle, websocket_abort_registration) = AbortHandle::new_pair();

        let chronicle = input.unwrap_or_else(|| self.build());

        let supervisor = chronicle
            .sender
            .clone()
            .ok_or_else(|| anyhow!("No supervisor for API!"))?;

        #[cfg(feature = "rocket_listener")]
        tokio::spawn(rocket_listener.start(Some(supervisor.clone())));

        // tokio::spawn(websocket.start_abortable(websocket_abort_registration, Some(supervisor.clone())));

        tokio::spawn(chronicle.start(Some(handle)));

        Ok(supervisor)
    }
}
