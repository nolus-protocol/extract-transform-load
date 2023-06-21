use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LP_Withdraw_Type {
    pub height: String,
    pub from: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "withdraw-amount")]
    pub withdraw_amount: String,
    #[serde(alias = "withdraw-symbol")]
    pub withdraw_symbol: String,
    pub receipts: String,
    pub close: String
}
