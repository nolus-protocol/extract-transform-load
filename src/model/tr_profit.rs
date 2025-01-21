use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

use crate::custom_uint::{UInt31, UInt63};

#[derive(Debug, FromRow)]
pub struct TR_Profit {
    pub TR_Profit_height: UInt63,
    pub TR_Profit_idx: Option<UInt31>,
    pub TR_Profit_timestamp: DateTime<Utc>,
    pub TR_Profit_amnt_stable: BigDecimal,
    pub TR_Profit_amnt_nls: BigDecimal,
    pub Tx_Hash: String,
}
