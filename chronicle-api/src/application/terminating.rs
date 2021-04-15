use super::*;

#[async_trait]
impl<H: ChronicleAPIScope> Terminating<H> for ChronicleAPI<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        status
    }
}
