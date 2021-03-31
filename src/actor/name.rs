#[async_trait::async_trait]
pub trait Name {
    async fn set_name(self) -> Self;
    async fn get_name(&self) -> String;
}
