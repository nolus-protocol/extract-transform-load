use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Opening_Type {
    pub id: String,
    pub customer: String,
    pub currency: String,
    pub air: String,
    pub at: String,
    #[serde(alias = "loan-pool-id")]
    pub loan_pool_id: String,
    #[serde(alias = "loan-amount")]
    pub loan_amount: String,
    #[serde(alias = "loan-symbol")]
    pub loan_symbol: String,
    #[serde(alias = "downpayment-amount")]
    pub downpayment_amount: String,
    #[serde(alias = "downpayment-symbol")]
    pub downpayment_symbol: String,
}
