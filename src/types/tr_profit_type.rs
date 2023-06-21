use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TR_Profit_Type {
    pub height: String,
    pub at: String,
    #[serde(alias = "profit-symbol")]
    pub profit_symbol: String,
    #[serde(alias = "profit-amount")]
    pub profit_amount: String,
}
