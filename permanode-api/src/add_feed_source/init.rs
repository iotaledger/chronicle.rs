use application::PermanodeSender;

use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> Init<PermanodeSender<H>> for AddFeedSource {
    async fn init(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeSender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
