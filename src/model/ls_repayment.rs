use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_Repayment {
    pub LS_repayment_height: i64,
    pub LS_repayment_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt: Option<BigDecimal>,
    pub LS_payment_amnt_stable: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_close: bool,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
    pub Tx_Hash: Option<String>,
}
