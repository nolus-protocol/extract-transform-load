use serde::Deserialize;

use super::AmountTicker;

#[derive(Debug, Deserialize)]
pub struct LS_State_Type {
    pub opened: Option<Status>,
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub amount: AmountTicker,
    pub loan_interest_rate: u128,
    pub margin_interest_rate: u128,
    pub principal_due: AmountTicker,
    pub previous_margin_due: AmountTicker,
    pub previous_interest_due: AmountTicker,
    pub current_margin_due: AmountTicker,
    pub current_interest_due: AmountTicker,
}
