use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct LP_Deposit_Type {
    pub height: String,
    pub from: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "deposit-amount")]
    pub deposit_amount: String,
    #[serde(alias = "deposit-symbol")]
    pub deposit_symbol: String,
    pub receipts: String,
}
