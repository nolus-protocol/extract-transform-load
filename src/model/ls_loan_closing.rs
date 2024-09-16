use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan_Closing {
    pub LS_contract_id: String,
    pub LS_symbol: String,
    pub LS_amnt: BigDecimal,
    pub LS_amnt_stable: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Type: String,
}
