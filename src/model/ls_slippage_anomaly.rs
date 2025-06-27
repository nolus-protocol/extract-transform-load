use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct LS_Slippage_Anomaly {
    pub Tx_Hash: Option<String>,
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_max_slipagge: i16,
    pub LS_timestamp: DateTime<Utc>,
}
