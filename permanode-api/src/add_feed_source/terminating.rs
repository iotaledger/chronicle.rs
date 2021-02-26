use super::*;

#[async_trait]
impl<H: PermanodeAPIScope> Terminating<PermanodeAPISender<H>> for AddFeedSource {
    async fn terminating(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeAPISender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}
