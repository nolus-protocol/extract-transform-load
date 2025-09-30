use serde::Deserialize;

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AmountTicker {
    pub amount: String,
    pub ticker: String,
}
