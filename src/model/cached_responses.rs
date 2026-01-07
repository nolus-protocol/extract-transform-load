use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Monthly active wallets data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyActiveWallet {
    pub month: String,
    pub unique_addresses: i64,
}

/// Revenue series data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevenueSeriesPoint {
    pub time: DateTime<Utc>,
    pub daily: BigDecimal,
    pub cumulative: BigDecimal,
}

/// Daily positions data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPositionsPoint {
    pub date: DateTime<Utc>,
    pub closed_loans: BigDecimal,
    pub opened_loans: BigDecimal,
}

/// Position bucket data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionBucket {
    pub loan_category: String,
    pub loan_count: i64,
    pub loan_size: BigDecimal,
}

/// Token loan data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLoan {
    pub symbol: String,
    pub value: BigDecimal,
}

/// Token position data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPosition {
    pub token: String,
    pub market_value: BigDecimal,
}
