use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeAPIBuilder<H>>> Terminating<H> for PermanodeAPI<H> {
    async fn terminating(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        println!("lol",);
        status
    }
}
