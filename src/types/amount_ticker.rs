use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct AmountTicker {
    pub amount: String,
    pub ticker: String,
}
