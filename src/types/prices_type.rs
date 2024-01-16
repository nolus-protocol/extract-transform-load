use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Prices {
    pub prices: Vec<AmountObject>,
}

#[derive(Debug, Deserialize)]
pub struct AmountObject {
    pub amount: Amount,
    pub amount_quote: Amount,
}

#[derive(Debug, Deserialize)]
pub struct Amount {
    pub amount: String,
    pub ticker: String,
}
