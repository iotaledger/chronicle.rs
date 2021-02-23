use super::*;
use crate::listener::{
    ListenerBuilder,
    WarpListener,
};
use std::borrow::Cow;

#[async_trait]
impl<H> Starter<H> for PermanodeBuilder<H>
where
    H: LauncherSender<PermanodeBuilder<H>>,
{
    type Ok = PermanodeSender<H>;

    type Error = Cow<'static, str>;

    type Input = Permanode<H>;

    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        let listener = ListenerBuilder::<WarpListener>::new().build();
        let (listener_handle, listener_abort_registration) = AbortHandle::new_pair();

        let permanode = self.listener_handle(listener_handle).build();

        let supervisor = permanode.sender.clone().unwrap();

        tokio::spawn(listener.start_abortable(listener_abort_registration, Some(supervisor.clone())));

        tokio::spawn(permanode.start(Some(handle)));

        Ok(supervisor)
    }
}
