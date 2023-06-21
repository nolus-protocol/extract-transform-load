use serde::Deserialize;

use super::AmountSymbol;

#[derive(Debug, Deserialize)]
pub struct LS_State_Type {
    pub opened: Option<Status>,
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub amount: AmountSymbol,
    pub interest_rate: u128,
    pub interest_rate_margin: u128,
    pub principal_due: AmountSymbol,
    pub previous_margin_due: AmountSymbol,
    pub previous_interest_due: AmountSymbol,
    pub current_margin_due: AmountSymbol,
    pub current_interest_due: AmountSymbol,
}

