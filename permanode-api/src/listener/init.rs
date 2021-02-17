use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> Init<PermanodeSender<H>> for Listener {
    async fn init(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
