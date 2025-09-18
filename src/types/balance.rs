use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Balance {
    pub amount: String,
}
