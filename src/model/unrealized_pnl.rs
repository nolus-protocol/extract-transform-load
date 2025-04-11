use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Unrealized_Pnl {
    #[sqlx(rename = "Daily Unrealized PnL")]
    pub amount: BigDecimal,
    #[sqlx(rename = "Day")]
    pub date: DateTime<Utc>,
}
