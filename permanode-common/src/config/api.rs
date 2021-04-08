use super::*;
/// Configuration for the Permanode API
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ApiConfig {}

impl ApiConfig {
    /// Verify that the api config is valid
    pub async fn verify(&mut self) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}
