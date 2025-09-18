use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan_Collect {
    pub LS_contract_id: String,
    pub LS_symbol: String,
    pub LS_amount: BigDecimal,
}
