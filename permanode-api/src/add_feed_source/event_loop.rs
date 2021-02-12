use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> EventLoop<PermanodeSender<H>> for AddFeedSource {
    async fn event_loop(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
