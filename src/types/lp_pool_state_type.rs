use serde::Deserialize;
use super::AmountSymbol;

#[derive(Debug, Deserialize)]
pub struct LP_Pool_State_Type {
    pub balance: AmountSymbol,
    pub total_principal_due: AmountSymbol,
    pub total_interest_due: AmountSymbol,
    pub balance_nlpn: AmountSymbol,
}
