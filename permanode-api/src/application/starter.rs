use super::*;
use crate::listener::ListenerBuilder;
#[cfg(feature = "rocket_listener")]
use crate::listener::RocketListener;
#[cfg(feature = "warp_listener")]
use crate::listener::WarpListener;
use std::borrow::Cow;

#[async_trait]
impl<H> Starter<H> for PermanodeAPIBuilder<H>
where
    H: PermanodeAPIScope,
{
    type Ok = PermanodeAPISender<H>;

    type Error = Cow<'static, str>;

    type Input = PermanodeAPI<H>;

    async fn starter(mut self, handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        #[cfg(feature = "rocket_listener")]
        let rocket_listener = {
            let rocket = rocket::ignite();
            let rocket_listener_handle = rocket.shutdown();
            let rocket_listener = ListenerBuilder::new()
                .data(RocketListener::new(rocket))
                .storage_config(self.storage_config.clone().expect("No storage config provided!"))
                .build();
            self = self.rocket_listener_handle(rocket_listener_handle);
            rocket_listener
        };

        #[cfg(feature = "warp_listener")]
        let warp_listener = {
            let (warp_listener_handle, warp_abort_handle) = tokio::sync::oneshot::channel();
            let warp_listener = ListenerBuilder::new()
                .data(WarpListener::new(warp_abort_handle))
                .storage_config(self.storage_config.clone().expect("No storage config provided!"))
                .build();
            self = self.warp_listener_handle(warp_listener_handle);
            warp_listener
        };

        // let websocket = WebsocketBuilder::new().build();
        // let (websocket_handle, websocket_abort_registration) = AbortHandle::new_pair();

        let permanode = self.build();

        let supervisor = permanode.sender.clone().unwrap();

        #[cfg(feature = "rocket_listener")]
        tokio::spawn(rocket_listener.start(Some(supervisor.clone())));

        #[cfg(feature = "warp_listener")]
        tokio::spawn(warp_listener.start(Some(supervisor.clone())));

        // tokio::spawn(websocket.start_abortable(websocket_abort_registration, Some(supervisor.clone())));

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
