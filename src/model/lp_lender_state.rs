use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LP_Lender_State {
    pub LP_Lender_id: String,
    pub LP_Pool_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Lender_stable: BigDecimal,
    pub LP_Lender_asset: BigDecimal,
    pub LP_Lender_receipts: BigDecimal,
}
