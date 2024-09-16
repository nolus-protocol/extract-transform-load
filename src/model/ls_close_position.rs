use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_Close_Position {
    pub Tx_Hash: Option<String>,
    pub LS_position_height: i64,
    pub LS_position_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_change: BigDecimal,
    pub LS_amount_amount: BigDecimal,
    pub LS_amount_symbol: String,

    pub LS_amnt_stable: Option<BigDecimal>,
    pub LS_payment_amnt: Option<BigDecimal>,
    pub LS_payment_symbol: Option<String>,
    pub LS_payment_amnt_stable: BigDecimal,

    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_close: bool,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
}
