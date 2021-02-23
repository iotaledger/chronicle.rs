use super::*;

#[async_trait]
impl<H: LauncherSender<PermanodeBuilder<H>>> Terminating<H> for Permanode<H> {
    async fn terminating(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        println!("lol",);
        status
    }
}
