//! Centralized cache key constants
//!
//! All cache keys used by controllers and cache_refresher are defined here
//! to prevent duplication and drift between the two locations.

// Metrics endpoints
pub const TVL: &str = "tvl";
pub const BORROWED_TOTAL: &str = "borrowed_total";
pub const SUPPLIED_FUNDS: &str = "supplied_funds";
pub const OPEN_INTEREST: &str = "open_interest";
pub const OPEN_POSITION_VALUE: &str = "open_position_value";

// Treasury endpoints
pub const REVENUE: &str = "revenue";
pub const BUYBACK_TOTAL: &str = "buyback_total";
pub const DISTRIBUTED: &str = "distributed";
pub const INCENTIVES_POOL: &str = "incentives_pool";

// PnL endpoints
pub const REALIZED_PNL_STATS: &str = "realized_pnl_stats";
pub const REALIZED_PNL_WALLET: &str = "realized_pnl_wallet_all";
pub const UNREALIZED_PNL: &str = "unrealized_pnl";

// Leases endpoints
pub const LEASES_MONTHLY: &str = "leases_monthly";
pub const LEASE_VALUE_STATS: &str = "lease_value_stats";
pub const LOANS_BY_TOKEN: &str = "loans_by_token";
pub const LOANS_GRANTED: &str = "loans_granted";
pub const LIQUIDATIONS: &str = "liquidations_all";
pub const INTEREST_REPAYMENTS: &str = "interest_repayments_all";
pub const HISTORICALLY_OPENED: &str = "historically_opened_all";
pub const HISTORICALLY_REPAID: &str = "historically_repaid";
pub const HISTORICALLY_LIQUIDATED: &str = "historically_liquidated";

// Positions endpoints
pub const POSITIONS: &str = "positions_all";
pub const POSITION_BUCKETS: &str = "position_buckets";
pub const OPEN_POSITIONS_BY_TOKEN: &str = "open_positions_by_token";
pub const DAILY_POSITIONS: &str = "daily_positions_3m_none";

// Liquidity endpoints
pub const POOLS: &str = "pools_all";
pub const CURRENT_LENDERS: &str = "current_lenders";
pub const HISTORICAL_LENDERS: &str = "historical_lenders_all";
pub const UTILIZATION_LEVEL_PROTOCOL: &str = "utilization_level_protocol";

// Misc endpoints
pub const TOTAL_TX_VALUE: &str = "total_tx_value";
pub const MONTHLY_ACTIVE_WALLETS: &str = "monthly_active_wallets";
pub const REVENUE_SERIES: &str = "revenue_series";
