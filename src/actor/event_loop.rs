use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait EventLoop<H>: Sized {
    async fn event_loop(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need>;
}
