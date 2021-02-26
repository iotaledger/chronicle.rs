use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Terminating<PermanodeAPISender<H>> for Notifications {
    async fn terminating(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeAPISender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
