use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Reserve_Cover_Loss {
    pub LS_contract_id: String,
    pub Tx_Hash: Option<String>,
    pub LS_symbol: String,
    pub LS_amnt: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Event_Block_Index: i32,
}
