use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Leases_Monthly {
    #[sqlx(rename = "Amount")]
    pub amount: BigDecimal,
    #[sqlx(rename = "Date")]
    pub date: DateTime<Utc>,
}
