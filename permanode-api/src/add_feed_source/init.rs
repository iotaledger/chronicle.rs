use super::*;

#[async_trait]
impl<H: PermanodeAPIScope> Init<PermanodeAPISender<H>> for AddFeedSource {
    async fn init(
        &mut self,
        status: Result<(), Need>,
        supervisor: &mut Option<PermanodeAPISender<H>>,
    ) -> Result<(), Need> {
        todo!()
    }
}