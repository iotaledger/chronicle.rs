use super::*;
/// Configuration for the Chronicle API
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ApiConfig {}

impl ApiConfig {
    /// Verify that the api config is valid
    pub async fn verify(&mut self) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}
