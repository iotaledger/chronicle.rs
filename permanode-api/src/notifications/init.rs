use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Init<PermanodeAPISender<H>> for Notifications {
    async fn init(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeAPISender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
