use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Liquidation_Type {
    pub height: String,
    pub to: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
    #[serde(alias = "amount-symbol")]
    pub amount_symbol: String,
    #[serde(alias = "amount-amount")]
    pub amount_amount: String,
    pub at: String,
    pub r#type: String,
    #[serde(alias = "prev-margin-interest")]
    pub prev_margin_interest: String,
    #[serde(alias = "prev-loan-interest")]
    pub prev_loan_interest: String,
    #[serde(alias = "curr-margin-interest")]
    pub curr_margin_interest: String,
    #[serde(alias = "curr-loan-interest")]
    pub curr_loan_interest: String,
    #[serde(alias = "loan-close")]
    pub loan_close: String,
    pub principal: String,
}
