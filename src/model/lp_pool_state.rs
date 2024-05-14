use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LP_Pool_State {
    pub LP_Pool_id: String,
    pub LP_Pool_timestamp: DateTime<Utc>,
    pub LP_Pool_total_value_locked_stable: BigDecimal,
    pub LP_Pool_total_value_locked_asset: BigDecimal,
    pub LP_Pool_total_issued_receipts: BigDecimal,
    pub LP_Pool_total_borrowed_stable: BigDecimal,
    pub LP_Pool_total_borrowed_asset: BigDecimal,
    pub LP_Pool_total_yield_stable: BigDecimal,
    pub LP_Pool_total_yield_asset: BigDecimal,
    pub LP_Pool_min_utilization_threshold: BigDecimal,
}
