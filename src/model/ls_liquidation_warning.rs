use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::custom_uint::UInt15;

#[derive(Debug, FromRow)]
pub struct LS_Liquidation_Warning {
    pub Tx_Hash: Option<String>,
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_level: UInt15,
    pub LS_ltv: UInt15,
    pub LS_timestamp: DateTime<Utc>,
}
