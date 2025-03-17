use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Unrealized_Pnl {
    pub pnl: BigDecimal,
    pub time: DateTime<Utc>,
}
