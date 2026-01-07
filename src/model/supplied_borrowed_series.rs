use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Supplied_Borrowed_Series {
    #[sqlx(rename = "LP_Pool_timestamp")]
    pub lp_pool_timestamp: DateTime<Utc>,
    #[sqlx(rename = "Supplied")]
    pub supplied: BigDecimal,
    #[sqlx(rename = "Borrowed")]
    pub borrowed: BigDecimal,
}
