use super::*;

#[async_trait]
impl<H: WebsocketScope> Terminating<H> for Websocket<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        status
    }
}
