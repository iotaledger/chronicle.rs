use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait Terminating<H: AknShutdown<Self>>: Sized {
    async fn terminating(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need>;
}
