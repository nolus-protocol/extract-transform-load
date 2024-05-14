use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Interest_values {
    pub prev_margin_interest: String,
    pub prev_loan_interest: String,
    pub curr_margin_interest: String,
    pub curr_loan_interest: String,
}
