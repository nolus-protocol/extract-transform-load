use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct LS_Auto_Close_Position {
    pub Tx_Hash: String,
    pub LS_contract_id: String,
    pub LS_Close_Strategy: String,
    pub LS_Close_Strategy_Ltv: i16,
    pub LS_timestamp: DateTime<Utc>,
}
