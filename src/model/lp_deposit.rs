use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LP_Deposit {
    pub LP_deposit_height: i64,
    pub LP_deposit_idx: Option<i32>,
    pub LP_address_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Pool_id: String,
    pub LP_amnt_stable: BigDecimal,
    pub LP_amnt_asset: BigDecimal,
    pub LP_amnt_receipts: BigDecimal,
}
