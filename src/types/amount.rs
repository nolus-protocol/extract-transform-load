use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Amount {
    pub amount: String,
}
