use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct TR_State {
    pub TR_timestamp: DateTime<Utc>,
    pub TR_amnt_stable: BigDecimal,
    pub TR_amnt_nls: BigDecimal,
}
