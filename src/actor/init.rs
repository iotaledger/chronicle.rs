use super::*;
use async_trait::async_trait;

#[async_trait]
pub trait Init<H>: Sized {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need>;
}
