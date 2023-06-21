use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct MP_Yield {
    pub MP_yield_symbol: String,
    pub MP_yield_timestamp: DateTime<Utc>,
    pub MP_apy_permilles: i32,
}
