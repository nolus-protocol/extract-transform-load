use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan_Closing {
    pub LS_contract_id: String,
    pub LS_amnt: BigDecimal,
    pub LS_amnt_stable: BigDecimal,
    pub LS_pnl: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Type: String,
    pub Block: i64,
    pub Active: bool,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan {
    pub LS_contract_id: String,
    pub LS_amnt: BigDecimal,
    pub LS_amnt_stable: BigDecimal,
    pub LS_pnl: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Active: bool,
}

#[derive(Debug, Deserialize, Serialize, FromRow)]
pub struct Pnl_Result {
    pub LS_contract_id: String,
    pub LS_amnt: BigDecimal,
    pub LS_amnt_stable: BigDecimal,
    pub LS_pnl: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Type: String,
    pub Block: i64,
    pub LS_asset_symbol: String,
    pub LS_loan_pool_id: String,
}

#[derive(Debug, Deserialize, Serialize, FromRow)]
pub struct Realized_Pnl_Result {
    pub LS_pnl: BigDecimal,
    pub LS_loan_pool_id: String,
    pub LS_asset_symbol: String,
}
