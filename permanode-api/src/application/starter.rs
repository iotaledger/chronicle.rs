use super::*;
use crate::listener::{
    ListenerBuilder,
    RocketListener,
    WarpListener,
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

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let storage_config = self
            .storage_config
            .clone()
            .ok_or("Tried to start application without Storage config!")?;
        let listener = ListenerBuilder::<RocketListener>::new().config(storage_config).build();
        let (listener_handle, listener_abort_registration) = AbortHandle::new_pair();

        let permanode = self.listener_handle(listener_handle).build();

        let supervisor = permanode.sender.clone().unwrap();

        tokio::spawn(listener.start_abortable(listener_abort_registration, Some(supervisor.clone())));

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
