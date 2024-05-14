use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct TR_Rewards_Distribution {
    pub TR_Rewards_height: i64,
    pub TR_Rewards_idx: Option<i32>,
    pub TR_Rewards_Pool_id: String,
    pub TR_Rewards_timestamp: DateTime<Utc>,
    pub TR_Rewards_amnt_stable: BigDecimal,
    pub TR_Rewards_amnt_nls: BigDecimal,
    pub Event_Block_Index: i32,
}
