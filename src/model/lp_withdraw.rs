use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LP_Withdraw {
    pub Tx_Hash: String,
    pub LP_withdraw_height: i64,
    pub LP_withdraw_idx: Option<i32>,
    pub LP_address_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Pool_id: String,
    pub LP_amnt_stable: BigDecimal,
    pub LP_amnt_asset: BigDecimal,
    pub LP_amnt_receipts: BigDecimal,
    pub LP_deposit_close: bool,
}
