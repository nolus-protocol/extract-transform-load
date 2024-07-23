use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_State {
    pub LS_contract_id: String,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_amnt_stable: BigDecimal,
    pub LS_amnt: BigDecimal,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
}
