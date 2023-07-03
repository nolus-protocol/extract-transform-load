use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AmountTicker {
    pub amount: String,
    pub ticker: String
}
