use serde::Deserialize;

use super::Amount;

#[derive(Debug, Deserialize)]
pub struct LP_Pool_State_Type {
    pub balance: Amount,
    pub total_principal_due: Amount,
    pub total_interest_due: Amount,
    pub balance_nlpn: Amount,
}
