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
        let storage_config = self
            .storage_config
            .clone()
            .ok_or("Tried to start application without Storage config!")?;

        let rocket = rocket::ignite();
        let listener_handle = rocket.shutdown();
        let listener = ListenerBuilder::new()
            .config(storage_config)
            .data(RocketListener::new(rocket))
            .build();

        let websocket = WebsocketBuilder::new().build();
        let (websocket_handle, websocket_abort_registration) = AbortHandle::new_pair();

        let permanode = self
            .listener_handle(listener_handle)
            .websocket_handle(websocket_handle)
            .build();

        let supervisor = permanode.sender.clone().unwrap();

        tokio::spawn(listener.start(Some(supervisor.clone())));

        tokio::spawn(websocket.start_abortable(websocket_abort_registration, Some(supervisor.clone())));

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
