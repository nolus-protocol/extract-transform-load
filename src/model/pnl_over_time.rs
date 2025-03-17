use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Pnl_Over_Time {
    #[sqlx(rename = "Hourly Unrealized PnL")]
    pub amount: BigDecimal,
    #[sqlx(rename = "Hour")]
    pub date: DateTime<Utc>,
}
