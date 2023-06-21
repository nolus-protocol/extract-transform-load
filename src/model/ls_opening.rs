use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_Opening {
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_interest: i16,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_pool_id: String,
    pub LS_loan_amnt_stable: BigDecimal,
    pub LS_loan_amnt_asset: BigDecimal,
    pub LS_cltr_symbol: String,
    pub LS_cltr_amnt_stable: BigDecimal,
    pub LS_cltr_amnt_asset: BigDecimal,
    pub LS_native_amnt_stable: BigDecimal,
    pub LS_native_amnt_nolus: BigDecimal,
}
