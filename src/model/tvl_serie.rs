use bigdecimal::BigDecimal;
use chrono::{Utc, DateTime};
use sqlx::FromRow;
use serde::{Serialize, Deserialize};

#[derive(Debug, FromRow, Deserialize, Serialize, Clone, Default)]
pub struct TVL_Serie {
    #[sqlx(rename = "TVL")]
    pub tvl: BigDecimal,
    #[sqlx(rename = "Timestamp")]
    pub timestamp: DateTime<Utc>,
}
