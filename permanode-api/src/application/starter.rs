use super::*;
use crate::{
    listener::{
        ListenerBuilder,
        RocketListener,
        WarpListener,
    },
    websocket::WebsocketBuilder,
};
use std::borrow::Cow;

#[async_trait]
impl<H> Starter<H> for PermanodeAPIBuilder<H>
where
    H: PermanodeAPIScope,
{
    type Ok = PermanodeAPISender<H>;

    type Error = Cow<'static, str>;

    type Input = PermanodeAPI<H>;

    async fn starter(self, handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let rocket = rocket::ignite();
        let rocket_listener_handle = rocket.shutdown();
        let rocket_listener = ListenerBuilder::new()
            .data(RocketListener::new(rocket))
            .storage_config(self.storage_config.clone().expect("No storage config provided!"))
            .build();

        // let (warp_listener_handle, warp_abort_registration) = AbortHandle::new_pair();
        // let warp_listener = ListenerBuilder::new().data(WarpListener).build();

        let websocket = WebsocketBuilder::new().build();
        let (websocket_handle, websocket_abort_registration) = AbortHandle::new_pair();

        let permanode = self
            .rocket_listener_handle(rocket_listener_handle)
            //.warp_listener_handle(warp_listener_handle)
            .websocket_handle(websocket_handle)
            .build();

        let supervisor = permanode.sender.clone().unwrap();

        tokio::spawn(rocket_listener.start(Some(supervisor.clone())));

        // tokio::spawn(warp_listener.start_abortable(warp_abort_registration, Some(supervisor.clone())));

        tokio::spawn(websocket.start_abortable(websocket_abort_registration, Some(supervisor.clone())));

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
