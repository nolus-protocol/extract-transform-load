use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

use crate::custom_uint::{UInt31, UInt63};

#[derive(Debug, FromRow)]
pub struct LP_Deposit {
    pub Tx_Hash: String,
    pub LP_deposit_height: UInt63,
    pub LP_deposit_idx: Option<UInt31>,
    pub LP_address_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Pool_id: String,
    pub LP_amnt_stable: BigDecimal,
    pub LP_amnt_asset: BigDecimal,
    pub LP_amnt_receipts: BigDecimal,
}
