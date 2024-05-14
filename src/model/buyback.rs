use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Buyback {
    #[sqlx(rename = "Bought-back")]
    pub bought_back: BigDecimal,
    #[sqlx(rename = "time")]
    pub timestamp: DateTime<Utc>,
}
