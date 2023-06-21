use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct TR_Profit {
    pub TR_Profit_height: i64,
    pub TR_Profit_idx: Option<i32>,
    pub TR_Profit_timestamp: DateTime<Utc>,
    pub TR_Profit_amnt_stable: BigDecimal,
    pub TR_Profit_amnt_nls: BigDecimal,
}
