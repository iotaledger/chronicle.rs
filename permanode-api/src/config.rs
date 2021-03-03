use serde::{
    Deserialize,
    Serialize,
};
/// Configuration for the Permanode API
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ApiConfig {}
