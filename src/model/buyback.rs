use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use serde::{Serialize, Deserialize};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Buyback {
    #[sqlx(rename = "Bought-back")]
    pub bought_back: BigDecimal,
    #[sqlx(rename = "time")]
    pub timestamp: DateTime<Utc>,
}
