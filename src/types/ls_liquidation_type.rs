use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Liquidation_Type {
    pub height: String,
    pub to: String,
    #[serde(alias = "liquidation-symbol")]
    pub liquidation_symbol: String,
    #[serde(alias = "liquidation-amount")]
    pub liquidation_amount: String,
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
    pub principal: String,
}