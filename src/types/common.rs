//! Consolidated types for blockchain events and API responses
//!
//! All types organized by domain sections.

use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use std::collections::HashMap;

// =============================================================================
// COMMON TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct Amount {
    pub amount: String,
}

#[derive(Debug, Deserialize)]
pub struct AmountSymbol {
    pub amount: String,
    pub symbol: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AmountTicker {
    pub amount: String,
    pub ticker: String,
}

#[derive(Debug, Deserialize)]
pub struct Balance {
    pub amount: String,
}

#[derive(Debug, Deserialize)]
pub struct Prices {
    pub prices: Vec<PriceAmountObject>,
}

#[derive(Debug, Deserialize)]
pub struct PriceAmountObject {
    pub amount: PriceAmount,
    pub amount_quote: PriceAmount,
}

#[derive(Debug, Deserialize)]
pub struct PriceAmount {
    pub amount: String,
    pub ticker: String,
}

#[derive(Debug, Deserialize)]
pub struct LPP_Price {
    pub amount: Amount,
    pub amount_quote: Amount,
}

#[derive(Debug, Deserialize)]
pub struct Interest_values {
    pub prev_margin_interest: String,
    pub prev_loan_interest: String,
    pub curr_margin_interest: String,
    pub curr_loan_interest: String,
}

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Bucket_Type {
    pub bucket: String,
    pub positions: i64,
    pub share_percent: BigDecimal,
}

#[derive(Debug, Clone)]
pub struct Currency(pub String, pub i16);

// =============================================================================
// BLOCKCHAIN RPC TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BlockValue {
    Block(BlockBody),
    NewBlock(NewBlockBody),
}

#[derive(Debug, Deserialize)]
pub struct BlockQuery {
    pub jsonrpc: String,
    pub id: i64,
    pub result: BlockQueryResult,
    pub error: Option<BodyError>,
}

#[derive(Debug, Deserialize)]
pub struct BlockQueryResult {
    pub block: BlockHeader,
    pub data: BlockData,
}

#[derive(Debug, Deserialize)]
pub struct BlockHeader {
    pub height: String,
}

#[derive(Debug, Deserialize)]
pub struct BlockData {
    pub txs: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct BlockBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: BlockResult,
    pub error: Option<BodyError>,
}

#[derive(Debug, Deserialize)]
pub struct BlockResult {
    pub height: String,
    pub txs_results: Option<Vec<TXS_RESULTS>>,
}

#[derive(Debug, Deserialize)]
pub struct TXS_RESULTS {
    pub events: Option<Vec<EventData>>,
}

#[derive(Debug, Deserialize)]
pub struct EventData {
    pub r#type: String,
    pub attributes: Vec<Attributes>,
}

#[derive(Debug, Deserialize)]
pub struct Attributes {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: NewBlockResult,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockResult {
    pub query: Option<String>,
    pub data: Option<NewBlockData>,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockData {
    pub r#type: String,
    pub value: Block,
}

#[derive(Debug, Deserialize)]
pub struct Block {
    pub block: Header,
    pub result_begin_block: ResultBeginBlock,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub header: NewBlockHeaderData,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockHeaderData {
    pub height: String,
}

#[derive(Debug, Deserialize)]
pub struct ResultBeginBlock {
    pub events: Option<Vec<EventData>>,
}

#[derive(Debug, Deserialize)]
pub struct AbciBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: AbciDataResult,
}

#[derive(Debug, Deserialize)]
pub struct AbciDataResult {
    pub response: AbciDataResponse,
}

#[derive(Debug, Deserialize)]
pub struct AbciDataResponse {
    pub data: String,
    pub version: String,
    pub last_block_height: String,
    pub last_block_app_hash: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: QueryResponse,
}

#[derive(Debug, Deserialize)]
pub struct QueryResponse {
    pub response: QueryParams,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub height: String,
    pub value: Option<String>,
    pub log: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MsgReceivePacket {
    pub amount: String,
    pub denom: String,
    pub receiver: String,
    pub sender: String,
}

#[derive(Debug, Deserialize)]
pub struct BodyError {
    pub code: String,
    pub message: String,
    pub data: String,
}

// =============================================================================
// EXTERNAL API TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct CoinGeckoInfo {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct CoinGeckoMarketData {
    pub prices: Vec<MarketData>,
    pub market_caps: Vec<MarketData>,
    pub total_volumes: Vec<MarketData>,
}

#[derive(Deserialize, Debug)]
pub struct MarketData(pub i64, pub f64);

pub type CoinGeckoPrice = HashMap<String, HashMap<String, f64>>;

#[derive(Debug, Deserialize)]
pub struct AdminProtocolType {
    pub network: String,
    pub contracts: ProtocolContracts,
}

#[derive(Debug, Deserialize)]
pub struct AdminProtocolExtendType {
    pub network: String,
    pub protocol: String,
    pub contracts: ProtocolContracts,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProtocolContracts {
    pub leaser: String,
    pub lpp: String,
    pub oracle: String,
    pub profit: String,
    #[serde(default)]
    pub reserve: Option<String>,
}

/// Response from admin contract {"platform":{}} query
#[derive(Debug, Deserialize)]
pub struct PlatformInfo {
    pub timealarms: String,
    pub treasury: String,
}

/// Response from oracle contract {"currencies":{}} query
/// Contains all currencies supported by a protocol's oracle
#[derive(Debug, Deserialize, Clone)]
pub struct OracleCurrency {
    pub ticker: String,
    pub bank_symbol: String,
    #[serde(default)]
    pub dex_symbol: Option<String>,
    pub decimal_digits: i16,
    pub group: String,
}

/// Extended admin protocol type with optional dex configuration
#[derive(Debug, Deserialize)]
pub struct AdminProtocolFullType {
    pub network: String,
    #[serde(default)]
    pub dex: Option<serde_json::Value>,
    pub contracts: ProtocolContracts,
}

// =============================================================================
// LEASE EVENT TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LS_Opening_Type {
    pub id: String,
    pub customer: String,
    pub currency: String,
    pub air: String,
    pub at: String,
    #[serde(alias = "loan-pool-id")]
    pub loan_pool_id: String,
    #[serde(alias = "loan-amount")]
    pub loan_amount: String,
    #[serde(alias = "loan-symbol")]
    pub loan_symbol: String,
    #[serde(alias = "downpayment-amount")]
    pub downpayment_amount: String,
    #[serde(alias = "downpayment-symbol")]
    pub downpayment_symbol: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_Closing_Type {
    pub id: String,
    pub at: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_Repayment_Type {
    pub height: String,
    pub to: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
    pub at: String,
    #[serde(alias = "loan-close")]
    pub loan_close: String,
    #[serde(alias = "prev-margin-interest")]
    pub prev_margin_interest: String,
    #[serde(alias = "prev-loan-interest")]
    pub prev_loan_interest: String,
    #[serde(alias = "curr-margin-interest")]
    pub curr_margin_interest: String,
    #[serde(alias = "curr-loan-interest")]
    pub curr_loan_interest: String,
    pub principal: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_Close_Position_Type {
    pub height: String,
    pub to: String,
    pub change: String,
    #[serde(alias = "amount-amount")]
    pub amount_amount: String,
    #[serde(alias = "amount-symbol")]
    pub amount_symbol: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
    pub at: String,
    #[serde(alias = "loan-close")]
    pub loan_close: String,
    #[serde(alias = "prev-margin-interest")]
    pub prev_margin_interest: String,
    #[serde(alias = "prev-loan-interest")]
    pub prev_loan_interest: String,
    #[serde(alias = "curr-margin-interest")]
    pub curr_margin_interest: String,
    #[serde(alias = "curr-loan-interest")]
    pub curr_loan_interest: String,
    pub principal: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_Liquidation_Type {
    pub height: String,
    pub to: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
    #[serde(alias = "amount-symbol")]
    pub amount_symbol: String,
    #[serde(alias = "amount-amount")]
    pub amount_amount: String,
    pub at: String,
    pub r#type: String,
    #[serde(alias = "prev-margin-interest")]
    pub prev_margin_interest: String,
    #[serde(alias = "prev-loan-interest")]
    pub prev_loan_interest: String,
    #[serde(alias = "curr-margin-interest")]
    pub curr_margin_interest: String,
    #[serde(alias = "curr-loan-interest")]
    pub curr_loan_interest: String,
    #[serde(alias = "loan-close")]
    pub loan_close: String,
    pub principal: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_Liquidation_Warning_Type {
    pub customer: String,
    pub lease: String,
    #[serde(alias = "lease-asset")]
    pub lease_asset: String,
    pub level: String,
    pub ltv: String,
}

#[derive(Debug, Deserialize)]
pub struct LS_State_Type {
    pub opened: Option<Status_Opened>,
    pub paid: Option<Status_Paid>,
    pub closing: Option<Status_Paid>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Status_Opened {
    pub amount: AmountTicker,
    pub loan_interest_rate: u128,
    pub margin_interest_rate: u128,
    pub principal_due: AmountTicker,
    pub previous_margin_due: Option<AmountTicker>,
    pub previous_interest_due: Option<AmountTicker>,
    pub current_margin_due: Option<AmountTicker>,
    pub current_interest_due: Option<AmountTicker>,
    pub overdue_margin: Option<AmountTicker>,
    pub overdue_interest: Option<AmountTicker>,
    pub due_margin: Option<AmountTicker>,
    pub due_interest: Option<AmountTicker>,
}

#[derive(Debug, Deserialize)]
pub struct Status_Paid {
    pub amount: AmountTicker,
}

#[derive(Debug, Deserialize)]
pub struct LS_Raw_State {
    pub FullClose: Option<TransferInInit>,
    pub PartialClose: Option<TransferInInit>,
    pub OpenedActive: Option<Lease>,
    pub ClosingTransferIn: Option<TransferInInit>,
    pub PaidActive: Option<Lease>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct TransferInInit {
    pub TransferInInit: Option<AmountIn>,
    pub TransferInFinish: Option<AmountIn>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AmountIn {
    pub amount_in: AmountTicker,
}

#[derive(Debug, Deserialize)]
pub struct Lease {
    pub lease: LeaseData,
}

#[derive(Debug, Deserialize)]
pub struct LeaseData {
    pub lease: PositionData,
}

#[derive(Debug, Deserialize)]
pub struct PositionData {
    pub position: Option<LeasePosition>,
    pub amount: Option<AmountTicker>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LeasePosition {
    pub amount: AmountTicker,
}

#[derive(Debug, Deserialize)]
pub struct LS_Auto_Close_Position_Type {
    pub to: String,
    #[serde(alias = "take-profit-ltv")]
    pub take_profit_ltv: Option<String>,
    #[serde(alias = "stop-loss-ltv")]
    pub stop_loss_ltv: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LS_Slippage_Anomaly_Type {
    pub customer: String,
    pub lease: String,
    #[serde(alias = "lease-asset")]
    pub lease_asset: String,
    pub max_slippage: String,
}

#[derive(Debug, Deserialize)]
pub struct Reserve_Cover_Loss_Type {
    pub to: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
}

// =============================================================================
// LIQUIDITY EVENT TYPES
// =============================================================================

#[derive(Debug, Deserialize, Default)]
pub struct LP_Deposit_Type {
    pub height: String,
    pub from: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "deposit-amount")]
    pub deposit_amount: String,
    #[serde(alias = "deposit-symbol")]
    pub deposit_symbol: String,
    pub receipts: String,
}

#[derive(Debug, Deserialize)]
pub struct LP_Withdraw_Type {
    pub height: String,
    pub from: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "withdraw-amount")]
    pub withdraw_amount: String,
    #[serde(alias = "withdraw-symbol")]
    pub withdraw_symbol: String,
    pub receipts: String,
    pub close: String,
}

#[derive(Debug, Deserialize)]
pub struct LP_Pool_State_Type {
    pub balance: Amount,
    pub total_principal_due: Amount,
    pub total_interest_due: Amount,
    pub balance_nlpn: Amount,
}

#[derive(Debug, Deserialize)]
pub struct LP_Lender_State_Type {
    pub balance: String,
    pub price: String,
}

#[derive(Debug, Deserialize)]
pub struct LP_Pool_Config_State_Type {
    pub borrow_rate: Borrow_Rate,
    pub min_utilization: u128,
}

#[derive(Debug, Deserialize)]
pub struct Borrow_Rate {
    pub addon_optimal_interest_rate: u128,
    pub base_interest_rate: u128,
    pub utilization_optimal: u128,
}

// =============================================================================
// TREASURY EVENT TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct TR_Profit_Type {
    pub height: String,
    pub at: String,
    #[serde(alias = "profit-symbol")]
    pub profit_symbol: String,
    #[serde(alias = "profit-amount")]
    pub profit_amount: String,
}

#[derive(Debug, Deserialize)]
pub struct TR_Rewards_Distribution_Type {
    pub height: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "rewards-symbol")]
    pub rewards_symbol: String,
    #[serde(alias = "rewards-amount")]
    pub rewards_amount: String,
}

#[derive(Debug, Deserialize)]
pub struct TR_State_Type {
    pub balances: Vec<(String,)>,
}
