use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Balance {
    pub balance: String,
}
