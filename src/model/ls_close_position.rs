use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

use crate::custom_uint::{UInt31, UInt63};

#[derive(Debug, FromRow, Clone)]
pub struct LS_Close_Position {
    pub Tx_Hash: String,
    pub LS_position_height: UInt63,
    pub LS_position_idx: Option<UInt31>,
    pub LS_contract_id: String,
    pub LS_change: BigDecimal,
    pub LS_amnt: BigDecimal,
    pub LS_amnt_symbol: String,

    pub LS_amnt_stable: BigDecimal,
    pub LS_payment_amnt: BigDecimal,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt_stable: BigDecimal,

    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_close: bool,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
}
