use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Opening {
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_interest: i16,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_pool_id: String,
    pub LS_loan_amnt: BigDecimal,
    pub LS_loan_amnt_stable: BigDecimal,
    pub LS_loan_amnt_asset: BigDecimal,
    pub LS_cltr_symbol: String,
    pub LS_cltr_amnt_stable: BigDecimal,
    pub LS_cltr_amnt_asset: BigDecimal,
    pub LS_native_amnt_stable: BigDecimal,
    pub LS_native_amnt_nolus: BigDecimal,
    pub LS_lpn_loan_amnt: BigDecimal,
    pub Tx_Hash: String,
    // Pre-computed columns for query optimization
    pub LS_position_type: Option<String>,
    pub LS_lpn_symbol: Option<String>,
    pub LS_lpn_decimals: Option<i64>,
    pub LS_opening_price: Option<BigDecimal>,
    pub LS_liquidation_price_at_open: Option<BigDecimal>,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_History {
    pub symbol: String,
    pub amount: BigDecimal,
    pub r#type: String,
    pub time: DateTime<Utc>,
    pub ls_amnt_symbol: Option<String>,
    pub ls_amnt: Option<BigDecimal>,
    pub additional: Option<String>,
}
