use super::*;
use std::borrow::Cow;
use futures::future::AbortHandle;
use tokio::net::TcpListener;

#[async_trait::async_trait]
impl<H> Starter<H> for BrokerBuilder<H>
where
    H: BrokerScope,
{
    type Ok = BrokerHandle<H>;

    type Error = Cow<'static, str>;

    type Input = PermanodeBroker<H>;

    async fn starter(mut self, handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        // create the listener if provided
        let mut listener_child = None;
        if let Some(listen_address) = self.listen_address.clone() {
            let tcp_listener = TcpListener::bind(listen_address)
                .await
                .map_err(|_| "Unable to bind to dashboard listen address")?;
            let listener = ListenerBuilder::new().tcp_listener(tcp_listener).build();
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let listener_handle = ListenerHandle::new(abort_handle);
            listener_child = Some((listener, abort_registration));
            // add listener handle to the broker application and build its state
            self = self.listener_handle(listener_handle);
        }

        let broker = self.build();

        let supervisor = broker.handle.clone().unwrap();
        // start listener(if provided) child in abortable mode
        if let Some((listener, abort_registration)) = listener_child {
            tokio::spawn(listener.start_abortable(abort_registration, Some(supervisor.clone())));
        }
        // start broker application
        tokio::spawn(broker.start(Some(handle)));

        Ok(supervisor)
    }
}
