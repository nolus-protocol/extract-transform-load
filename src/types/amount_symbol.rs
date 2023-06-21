use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AmountSymbol {
    pub amount: String,
    pub symbol: String,
}
