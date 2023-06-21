use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct MP_Asset_State {
    pub MP_asset_symbol: String,
    pub MP_timestamp: DateTime<Utc>,
    pub MP_price_open: BigDecimal,
    pub MP_price_high: BigDecimal,
    pub MP_price_low: BigDecimal,
    pub MP_price_close: BigDecimal,
    pub MP_volume: BigDecimal,
    pub MP_marketcap: BigDecimal,
}
