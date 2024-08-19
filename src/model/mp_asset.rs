use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct MP_Asset {
    pub MP_asset_symbol: String,
    pub MP_asset_timestamp: DateTime<Utc>,
    pub MP_price_in_stable: BigDecimal,
    pub Protocol: String,
}
