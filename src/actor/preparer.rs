use async_trait::async_trait;

#[async_trait]
pub trait Preparer<I> {
    type Ok;
    type Error;
    async fn preparer(&mut self, input: I) -> Result<Self::Ok, Self::Error>;
}
