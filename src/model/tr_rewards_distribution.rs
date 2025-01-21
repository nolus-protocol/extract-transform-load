use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

use crate::custom_uint::{UInt31, UInt63};

#[derive(Debug, FromRow)]
pub struct TR_Rewards_Distribution {
    pub TR_Rewards_height: UInt63,
    pub TR_Rewards_idx: Option<UInt31>,
    pub TR_Rewards_Pool_id: String,
    pub TR_Rewards_timestamp: DateTime<Utc>,
    pub TR_Rewards_amnt_stable: BigDecimal,
    pub TR_Rewards_amnt_nls: BigDecimal,
    pub Event_Block_Index: UInt31,
    pub Tx_Hash: String,
}
