use async_trait::async_trait;

#[async_trait]
pub trait Starter<H> {
    type Ok;
    type Error: std::fmt::Display;
    type Input;
    async fn starter(self, handle: H, input: Option<Self::Input>) -> Result<Self::Ok, Self::Error>;
}
