//! Consolidated database models
//!
//! All database entity structs organized by domain sections.

use std::{fmt, io, str::FromStr};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use cosmrs::proto::{tendermint::abci::Event, Timestamp};
use cosmrs::{tx::Fee, Any};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal as SqlxBigDecimal, FromRow};

// =============================================================================
// LEASE DOMAIN
// =============================================================================

// -----------------------------------------------------------------------------
// Core Lease Structs
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Opening {
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_interest: i16,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_pool_id: String,
    pub LS_loan_amnt: SqlxBigDecimal,
    pub LS_loan_amnt_stable: SqlxBigDecimal,
    pub LS_loan_amnt_asset: SqlxBigDecimal,
    pub LS_cltr_symbol: String,
    pub LS_cltr_amnt_stable: SqlxBigDecimal,
    pub LS_cltr_amnt_asset: SqlxBigDecimal,
    pub LS_native_amnt_stable: SqlxBigDecimal,
    pub LS_native_amnt_nolus: SqlxBigDecimal,
    pub LS_lpn_loan_amnt: SqlxBigDecimal,
    pub Tx_Hash: String,
    pub LS_position_type: Option<String>,
    pub LS_lpn_symbol: Option<String>,
    pub LS_lpn_decimals: Option<i64>,
    pub LS_opening_price: Option<SqlxBigDecimal>,
    pub LS_liquidation_price_at_open: Option<SqlxBigDecimal>,
}

#[derive(Debug, FromRow)]
pub struct LS_Closing {
    pub Tx_Hash: String,
    pub LS_contract_id: String,
    pub LS_timestamp: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct LS_State {
    pub LS_contract_id: String,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_amnt_stable: SqlxBigDecimal,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_prev_margin_stable: SqlxBigDecimal,
    pub LS_prev_interest_stable: SqlxBigDecimal,
    pub LS_current_margin_stable: SqlxBigDecimal,
    pub LS_current_interest_stable: SqlxBigDecimal,
    pub LS_principal_stable: SqlxBigDecimal,
    pub LS_lpn_loan_amnt: SqlxBigDecimal,
    pub LS_prev_margin_asset: SqlxBigDecimal,
    pub LS_prev_interest_asset: SqlxBigDecimal,
    pub LS_current_margin_asset: SqlxBigDecimal,
    pub LS_current_interest_asset: SqlxBigDecimal,
    pub LS_principal_asset: SqlxBigDecimal,
}

// -----------------------------------------------------------------------------
// Lease Transactions
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow)]
pub struct LS_Repayment {
    pub LS_repayment_height: i64,
    pub LS_repayment_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt: SqlxBigDecimal,
    pub LS_payment_amnt_stable: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_close: bool,
    pub LS_prev_margin_stable: SqlxBigDecimal,
    pub LS_prev_interest_stable: SqlxBigDecimal,
    pub LS_current_margin_stable: SqlxBigDecimal,
    pub LS_current_interest_stable: SqlxBigDecimal,
    pub LS_principal_stable: SqlxBigDecimal,
    pub Tx_Hash: String,
}

#[derive(Debug, FromRow, Clone)]
pub struct LS_Close_Position {
    pub Tx_Hash: String,
    pub LS_position_height: i64,
    pub LS_position_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_change: SqlxBigDecimal,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_amnt_symbol: String,
    pub LS_amnt_stable: SqlxBigDecimal,
    pub LS_payment_amnt: SqlxBigDecimal,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt_stable: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_loan_close: bool,
    pub LS_prev_margin_stable: SqlxBigDecimal,
    pub LS_prev_interest_stable: SqlxBigDecimal,
    pub LS_current_margin_stable: SqlxBigDecimal,
    pub LS_current_interest_stable: SqlxBigDecimal,
    pub LS_principal_stable: SqlxBigDecimal,
}

// -----------------------------------------------------------------------------
// Liquidation
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow)]
pub struct LS_Liquidation {
    pub Tx_Hash: String,
    pub LS_liquidation_height: i64,
    pub LS_liquidation_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_amnt_symbol: String,
    pub LS_amnt_stable: SqlxBigDecimal,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt: SqlxBigDecimal,
    pub LS_payment_amnt_stable: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_transaction_type: String,
    pub LS_prev_margin_stable: SqlxBigDecimal,
    pub LS_prev_interest_stable: SqlxBigDecimal,
    pub LS_current_margin_stable: SqlxBigDecimal,
    pub LS_current_interest_stable: SqlxBigDecimal,
    pub LS_principal_stable: SqlxBigDecimal,
    pub LS_loan_close: bool,
    /// Price of the liquidated asset at the time of liquidation (stored for fast queries)
    pub LS_liquidation_price: Option<SqlxBigDecimal>,
}

#[derive(Debug, FromRow)]
pub struct LS_Liquidation_Warning {
    pub Tx_Hash: Option<String>,
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_level: i16,
    pub LS_ltv: i16,
    pub LS_timestamp: DateTime<Utc>,
}

#[derive(sqlx::Type, Debug)]
pub enum LS_transactions {
    Interest_Overdue_Liquidation,
    Liability_Exceeded_Liquidation,
}

impl fmt::Display for LS_transactions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LS_transactions::Interest_Overdue_Liquidation => write!(f, "0"),
            LS_transactions::Liability_Exceeded_Liquidation => write!(f, "1"),
        }
    }
}

impl From<LS_transactions> for String {
    fn from(value: LS_transactions) -> Self {
        match value {
            LS_transactions::Interest_Overdue_Liquidation => String::from("0"),
            LS_transactions::Liability_Exceeded_Liquidation => {
                String::from("1")
            },
        }
    }
}

impl FromStr for LS_transactions {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<LS_transactions, Self::Err> {
        match value {
            "0" => Ok(LS_transactions::Interest_Overdue_Liquidation),
            "1" => Ok(LS_transactions::Liability_Exceeded_Liquidation),
            _ => Err(io::Error::other("LS_transactions not supported")),
        }
    }
}

pub enum LS_Liquidation_Type {
    OverdueInterest,
    HighLiability,
    Unsupported,
}

impl fmt::Display for LS_Liquidation_Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LS_Liquidation_Type::OverdueInterest => {
                write!(f, "overdue interest")
            },
            LS_Liquidation_Type::HighLiability => write!(f, "high liability"),
            LS_Liquidation_Type::Unsupported => write!(f, "unsupported"),
        }
    }
}

impl From<LS_Liquidation_Type> for String {
    fn from(value: LS_Liquidation_Type) -> Self {
        match value {
            LS_Liquidation_Type::OverdueInterest => {
                String::from("overdue interest")
            },
            LS_Liquidation_Type::HighLiability => {
                String::from("high liability")
            },
            LS_Liquidation_Type::Unsupported => String::from("unsupported"),
        }
    }
}

impl From<&str> for LS_Liquidation_Type {
    fn from(value: &str) -> LS_Liquidation_Type {
        match value {
            "overdue interest" => LS_Liquidation_Type::OverdueInterest,
            "high liability" => LS_Liquidation_Type::HighLiability,
            _ => LS_Liquidation_Type::Unsupported,
        }
    }
}

// -----------------------------------------------------------------------------
// Loan Closing & PnL
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow, Deserialize, Serialize, Clone)]
pub struct LS_Loan_Closing {
    pub LS_contract_id: String,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_amnt_stable: SqlxBigDecimal,
    pub LS_pnl: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Type: String,
    pub Block: i64,
    pub Active: bool,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan {
    pub LS_contract_id: String,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_amnt_stable: SqlxBigDecimal,
    pub LS_pnl: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Active: bool,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Pnl_Result {
    #[sqlx(rename = "Position ID")]
    pub LS_contract_id: String,
    pub LS_asset_symbol: String,
    pub LS_loan_pool_id: String,
    pub Type: String,
    pub LS_timestamp: DateTime<Utc>,
    #[sqlx(rename = "Sent (USDC, Opening)")]
    pub Ls_sent: f64,
    #[sqlx(rename = "Received (USDC, Closing)")]
    pub Ls_receive: f64,
    #[sqlx(rename = "Realized PnL (USDC)")]
    pub LS_pnl: f64,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Realized_Pnl_Data {
    #[serde(rename = "Position ID")]
    #[sqlx(rename = "Position ID")]
    pub Position_Id: String,
    #[serde(rename = "Sent Amount")]
    #[sqlx(rename = "Sent Amount")]
    pub Sent_Amount: SqlxBigDecimal,
    #[serde(rename = "Sent Currency")]
    #[sqlx(rename = "Sent Currency")]
    pub Sent_Currency: String,
    #[serde(rename = "Received Amount")]
    #[sqlx(rename = "Received Amount")]
    pub Received_Amount: SqlxBigDecimal,
    #[serde(rename = "Received Currency")]
    #[sqlx(rename = "Received Currency")]
    pub Received_Currency: String,
    #[serde(rename = "Fee Amount")]
    #[sqlx(rename = "Fee Amount")]
    pub Fee_Amount: SqlxBigDecimal,
    #[serde(rename = "Fee Currency")]
    #[sqlx(rename = "Fee Currency")]
    pub Fee_Currency: String,
    pub Label: String,
    pub Description: String,
    pub TxHash: String,
    pub Date: DateTime<Utc>,
}

// -----------------------------------------------------------------------------
// Other Lease-Related Structs
// -----------------------------------------------------------------------------

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Loan_Collect {
    pub LS_contract_id: String,
    pub LS_symbol: String,
    pub LS_amount: SqlxBigDecimal,
    pub LS_amount_stable: SqlxBigDecimal,
}

#[derive(Debug, FromRow)]
pub struct LS_Auto_Close_Position {
    pub Tx_Hash: String,
    pub LS_contract_id: String,
    pub LS_Close_Strategy: String,
    pub LS_Close_Strategy_Ltv: i16,
    pub LS_timestamp: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct LS_Slippage_Anomaly {
    pub Tx_Hash: Option<String>,
    pub LS_contract_id: String,
    pub LS_address_id: String,
    pub LS_asset_symbol: String,
    pub LS_max_slipagge: i16,
    pub LS_timestamp: DateTime<Utc>,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_History {
    pub symbol: String,
    pub amount: SqlxBigDecimal,
    pub r#type: String,
    pub time: DateTime<Utc>,
    pub ls_amnt_symbol: Option<String>,
    pub ls_amnt: Option<SqlxBigDecimal>,
    pub additional: Option<String>,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Amount {
    pub amount: SqlxBigDecimal,
    pub time: DateTime<Utc>,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Reserve_Cover_Loss {
    pub LS_contract_id: String,
    pub Tx_Hash: String,
    pub LS_symbol: String,
    pub LS_amnt: SqlxBigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub Event_Block_Index: i32,
}

// =============================================================================
// LIQUIDITY DOMAIN
// =============================================================================

#[derive(Debug, FromRow)]
pub struct LP_Pool {
    pub LP_Pool_id: String,
    pub LP_symbol: String,
    pub LP_status: bool,
}

#[derive(Debug, FromRow)]
pub struct LP_Deposit {
    pub Tx_Hash: String,
    pub LP_deposit_height: i64,
    pub LP_deposit_idx: Option<i32>,
    pub LP_address_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Pool_id: String,
    pub LP_amnt_stable: SqlxBigDecimal,
    pub LP_amnt_asset: SqlxBigDecimal,
    pub LP_amnt_receipts: SqlxBigDecimal,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LP_Withdraw {
    pub Tx_Hash: String,
    pub LP_withdraw_height: i64,
    pub LP_withdraw_idx: Option<i32>,
    pub LP_address_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Pool_id: String,
    pub LP_amnt_stable: SqlxBigDecimal,
    pub LP_amnt_asset: SqlxBigDecimal,
    pub LP_amnt_receipts: SqlxBigDecimal,
    pub LP_deposit_close: bool,
}

#[derive(Debug, FromRow)]
pub struct LP_Pool_State {
    pub LP_Pool_id: String,
    pub LP_Pool_timestamp: DateTime<Utc>,
    pub LP_Pool_total_value_locked_stable: SqlxBigDecimal,
    pub LP_Pool_total_value_locked_asset: SqlxBigDecimal,
    pub LP_Pool_total_issued_receipts: SqlxBigDecimal,
    pub LP_Pool_total_borrowed_stable: SqlxBigDecimal,
    pub LP_Pool_total_borrowed_asset: SqlxBigDecimal,
    pub LP_Pool_total_yield_stable: SqlxBigDecimal,
    pub LP_Pool_total_yield_asset: SqlxBigDecimal,
    pub LP_Pool_min_utilization_threshold: SqlxBigDecimal,
}

#[derive(Debug, FromRow)]
pub struct LP_Lender_State {
    pub LP_Lender_id: String,
    pub LP_Pool_id: String,
    pub LP_timestamp: DateTime<Utc>,
    pub LP_Lender_stable: SqlxBigDecimal,
    pub LP_Lender_asset: SqlxBigDecimal,
    pub LP_Lender_receipts: SqlxBigDecimal,
}

// =============================================================================
// TREASURY DOMAIN
// =============================================================================

#[derive(Debug, FromRow)]
pub struct TR_Profit {
    pub TR_Profit_height: i64,
    pub TR_Profit_idx: Option<i32>,
    pub TR_Profit_timestamp: DateTime<Utc>,
    pub TR_Profit_amnt_stable: SqlxBigDecimal,
    pub TR_Profit_amnt_nls: SqlxBigDecimal,
    pub Tx_Hash: String,
}

#[derive(Debug, FromRow)]
pub struct TR_Rewards_Distribution {
    pub TR_Rewards_height: i64,
    pub TR_Rewards_idx: Option<i32>,
    pub TR_Rewards_Pool_id: String,
    pub TR_Rewards_timestamp: DateTime<Utc>,
    pub TR_Rewards_amnt_stable: SqlxBigDecimal,
    pub TR_Rewards_amnt_nls: SqlxBigDecimal,
    pub Event_Block_Index: i32,
    pub Tx_Hash: String,
}

#[derive(Debug, FromRow)]
pub struct TR_State {
    pub TR_timestamp: DateTime<Utc>,
    pub TR_amnt_stable: SqlxBigDecimal,
    pub TR_amnt_nls: SqlxBigDecimal,
}

// =============================================================================
// MARKET DOMAIN
// =============================================================================

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct MP_Asset {
    pub MP_asset_symbol: String,
    pub MP_asset_timestamp: DateTime<Utc>,
    pub MP_price_in_stable: SqlxBigDecimal,
    pub Protocol: String,
}

#[derive(Debug, FromRow)]
pub struct MP_Yield {
    pub MP_yield_symbol: String,
    pub MP_yield_timestamp: DateTime<Utc>,
    pub MP_apy_permilles: i32,
}

// =============================================================================
// PLATFORM DOMAIN
// =============================================================================

#[derive(Debug, FromRow)]
pub struct Block {
    pub id: i64,
}

#[derive(Debug, FromRow)]
pub struct Action_History {
    pub action_type: String,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::Type, Debug)]
pub enum Actions {
    MpAssetAction,
    AggregationAction,
}

impl fmt::Display for Actions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Actions::MpAssetAction => write!(f, "0"),
            Actions::AggregationAction => write!(f, "1"),
        }
    }
}

impl From<Actions> for String {
    fn from(value: Actions) -> Self {
        match value {
            Actions::MpAssetAction => String::from("0"),
            Actions::AggregationAction => String::from("1"),
        }
    }
}

impl FromStr for Actions {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Actions, Self::Err> {
        match value {
            "0" => Ok(Actions::MpAssetAction),
            "1" => Ok(Actions::AggregationAction),
            _ => Err(io::Error::other("Action Type not supported")),
        }
    }
}

#[derive(Debug, FromRow)]
pub struct PL_State {
    pub PL_timestamp: DateTime<Utc>,
    pub PL_pools_TVL_stable: SqlxBigDecimal,
    pub PL_pools_borrowed_stable: SqlxBigDecimal,
    pub PL_pools_yield_stable: SqlxBigDecimal,
    pub PL_LS_count_open: i64,
    pub PL_LS_count_closed: i64,
    pub PL_LS_count_opened: i64,
    pub PL_IN_LS_cltr_amnt_opened_stable: SqlxBigDecimal,
    pub PL_LP_count_open: i64,
    pub PL_LP_count_closed: i64,
    pub PL_LP_count_opened: i64,
    pub PL_OUT_LS_loan_amnt_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_amnt_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_prev_margin_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_prev_interest_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_current_margin_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_current_interest_stable: SqlxBigDecimal,
    pub PL_IN_LS_rep_principal_stable: SqlxBigDecimal,
    pub PL_OUT_LS_cltr_amnt_stable: SqlxBigDecimal,
    pub PL_OUT_LS_amnt_stable: SqlxBigDecimal,
    pub PL_native_amnt_stable: SqlxBigDecimal,
    pub PL_native_amnt_nolus: SqlxBigDecimal,
    pub PL_IN_LP_amnt_stable: SqlxBigDecimal,
    pub PL_OUT_LP_amnt_stable: SqlxBigDecimal,
    pub PL_TR_profit_amnt_stable: SqlxBigDecimal,
    pub PL_TR_profit_amnt_nls: SqlxBigDecimal,
    pub PL_TR_tax_amnt_stable: SqlxBigDecimal,
    pub PL_TR_tax_amnt_nls: SqlxBigDecimal,
    pub PL_OUT_TR_rewards_amnt_stable: SqlxBigDecimal,
    pub PL_OUT_TR_rewards_amnt_nls: SqlxBigDecimal,
}

#[derive(Debug, Clone, FromRow)]
pub struct Pool_Config {
    pub pool_id: String,
    pub position_type: String,
    pub lpn_symbol: String,
    pub lpn_decimals: i64,
    pub label: String,
    pub is_active: bool,
    pub first_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    pub deprecated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub stable_currency_symbol: Option<String>,
    pub stable_currency_decimals: Option<i64>,
}

pub struct PoolConfigUpsert<'a> {
    pub pool_id: &'a str,
    pub position_type: &'a str,
    pub lpn_symbol: &'a str,
    pub lpn_decimals: i64,
    pub label: &'a str,
    pub protocol: &'a str,
    pub stable_currency_symbol: &'a str,
    pub stable_currency_decimals: i64,
}

// =============================================================================
// DYNAMIC CONFIGURATION REGISTRY TYPES
// =============================================================================

/// Currency registry entry - stores all currencies ever seen (active and deprecated)
#[derive(Debug, Clone, FromRow)]
pub struct CurrencyRegistry {
    pub ticker: String,
    pub bank_symbol: Option<String>,
    pub decimal_digits: i16,
    pub is_active: bool,
    pub first_seen_at: DateTime<Utc>,
    pub deprecated_at: Option<DateTime<Utc>>,
}

/// Junction table: which protocols use each currency, and in what role (group)
#[derive(Debug, Clone, FromRow)]
pub struct CurrencyProtocol {
    pub ticker: String,
    pub protocol: String,
    pub group: Option<String>,
}

/// Protocol registry entry - stores all protocols ever seen (active and deprecated)
#[derive(Debug, Clone, FromRow)]
pub struct ProtocolRegistry {
    pub protocol_name: String,
    pub network: Option<String>,
    pub dex: Option<String>,
    pub leaser_contract: Option<String>,
    pub lpp_contract: Option<String>,
    pub oracle_contract: Option<String>,
    pub profit_contract: Option<String>,
    pub reserve_contract: Option<String>,
    pub lpn_symbol: String,
    pub position_type: String,
    pub is_active: bool,
    pub first_seen_at: DateTime<Utc>,
    pub deprecated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
pub struct Subscription {
    pub active: Option<bool>,
    pub address: String,
    pub p256dh: String,
    pub auth: String,
    pub endpoint: String,
    pub expiration: Option<DateTime<Utc>>,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
}

// =============================================================================
// API RESPONSE TYPES
// =============================================================================

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Borrow_APR {
    pub APR: BigDecimal,
}

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Buyback {
    #[sqlx(rename = "Bought-back")]
    pub bought_back: BigDecimal,
    #[sqlx(rename = "time")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Leases_Monthly {
    #[sqlx(rename = "Amount")]
    pub amount: BigDecimal,
    #[sqlx(rename = "Date")]
    pub date: DateTime<Utc>,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Pnl_Over_Time {
    #[sqlx(rename = "Hourly Unrealized PnL")]
    pub amount: BigDecimal,
    #[sqlx(rename = "Hour")]
    pub date: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Supplied_Borrowed_Series {
    #[sqlx(rename = "LP_Pool_timestamp")]
    pub lp_pool_timestamp: DateTime<Utc>,
    #[sqlx(rename = "Supplied")]
    pub supplied: BigDecimal,
    #[sqlx(rename = "Borrowed")]
    pub borrowed: BigDecimal,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Unrealized_Pnl {
    pub pnl: BigDecimal,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Leased_Asset {
    #[sqlx(rename = "Loan")]
    pub loan: BigDecimal,
    #[sqlx(rename = "Asset")]
    pub asset: String,
}

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Position {
    #[sqlx(rename = "Date")]
    pub date: String,
    #[sqlx(rename = "Type")]
    pub position_type: String,
    #[sqlx(rename = "Symbol")]
    pub symbol: String,
    #[sqlx(rename = "Asset")]
    pub asset: String,
    #[sqlx(rename = "Contract ID")]
    pub contract_id: String,
    #[sqlx(rename = "User")]
    pub user: String,
    #[sqlx(rename = "Loan")]
    pub loan: BigDecimal,
    #[sqlx(rename = "Down Payment")]
    pub down_payment: BigDecimal,
    #[sqlx(rename = "Lease Value")]
    pub lease_value: BigDecimal,
    #[sqlx(rename = "PnL")]
    pub pnl: Option<BigDecimal>,
    #[sqlx(rename = "PnL %")]
    pub pnl_percent: Option<BigDecimal>,
    #[sqlx(rename = "Current Price")]
    pub current_price: Option<BigDecimal>,
    #[sqlx(rename = "Liquidation Price")]
    pub liquidation_price: Option<BigDecimal>,
}

// -----------------------------------------------------------------------------
// Cached Response Types
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyActiveWallet {
    pub month: String,
    pub unique_addresses: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevenueSeriesPoint {
    pub time: DateTime<Utc>,
    pub daily: BigDecimal,
    pub cumulative: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPositionsPoint {
    pub date: DateTime<Utc>,
    pub closed_loans: BigDecimal,
    pub opened_loans: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionBucket {
    pub loan_category: String,
    pub loan_count: i64,
    pub loan_size: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLoan {
    pub symbol: String,
    pub value: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPosition {
    pub token: String,
    pub market_value: BigDecimal,
}

// -----------------------------------------------------------------------------
// Parameter Types
// -----------------------------------------------------------------------------

pub struct RawTxParams<'a> {
    pub tx_hash: String,
    pub tx_data: Any,
    pub height: i64,
    pub code: u32,
    pub time_stamp: Timestamp,
    pub tx_events: &'a [Event],
}

pub struct RawMsgParams<'a> {
    pub index: i32,
    pub value: Any,
    pub tx_hash: String,
    pub block: i64,
    pub time_stamp: Timestamp,
    pub fee: Fee,
    pub memo: String,
    pub events: Vec<String>,
    pub tx_events: &'a [Event],
    pub code: u32,
}
