//! Background cache refresh task
//!
//! This module provides proactive cache refresh for API endpoints.
//! It runs in the background, checking which caches are about to expire
//! and refreshing them with controlled concurrency to avoid DB load spikes.
//!
//! Configuration (via .env):
//! - CACHE_REFRESH_INTERVAL_SECS: How often to check for stale caches (default: 30)
//! - CACHE_MAX_CONCURRENT_REFRESHES: Max parallel refreshes during operation (default: 4)
//! - CACHE_MAX_CONCURRENT_INITIAL_REFRESHES: Max parallel refreshes at startup (default: 6)
//!
//! ## Adding a New Cache
//!
//! To add a new cached endpoint, add a single entry to the `define_caches!` macro below.
//! The macro generates all necessary dispatch code automatically.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};

use bigdecimal::BigDecimal;
use futures::stream::{self, StreamExt};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use etl_core::{
    cache_keys,
    configuration::{AppState, State, CACHE_TTL_HOURLY, CACHE_TTL_STANDARD},
    dao::postgre::{
        lp_pool_state::PoolUtilizationLevel, ls_opening::RealizedPnlWallet,
    },
    error::Error,
    helpers::build_protocol_cache_key,
    model::{
        DailyPositionsPoint, MonthlyActiveWallet, PositionBucket,
        RevenueSeriesPoint, TokenLoan, TokenPosition,
    },
};

/// Tracks when each cache was last refreshed so we can proactively
/// refresh before Moka's TTL expires.
struct RefreshTracker {
    timestamps: RwLock<HashMap<String, Instant>>,
}

/// Refresh when 80% of TTL has elapsed
const REFRESH_THRESHOLD: f64 = 0.8;

impl RefreshTracker {
    fn new() -> Self {
        Self {
            timestamps: RwLock::new(HashMap::new()),
        }
    }

    async fn record_refresh(&self, key: &str) {
        let mut ts = self.timestamps.write().await;
        ts.insert(key.to_string(), Instant::now());
    }

    async fn needs_refresh(&self, key: &str, ttl: Duration) -> bool {
        let ts = self.timestamps.read().await;
        match ts.get(key) {
            Some(last) => last.elapsed() > ttl.mul_f64(REFRESH_THRESHOLD),
            None => true, // Never refreshed — needs initial population
        }
    }
}

/// Macro to define all caches in a single place.
///
/// Each entry maps: cache_key_constant => cache_field, ttl_constant
///
/// The macro generates:
/// - ALL_CACHE_NAMES: Array of all cache key constants
/// - ttl_for_cache(): Returns the TTL Duration for a given cache name
macro_rules! define_caches {
    (
        $(
            $key:ident => $field:ident, $ttl:expr
        ),* $(,)?
    ) => {
        /// All cache names for iteration (auto-generated from define_caches!)
        const ALL_CACHE_NAMES: &[&str] = &[
            $(cache_keys::$key),*
        ];

        /// Get the TTL duration for a cache by name (auto-generated)
        fn ttl_for_cache(cache_name: &str) -> Duration {
            $(
                if cache_name == cache_keys::$key {
                    return $ttl;
                }
            )*
            CACHE_TTL_STANDARD // unreachable for known caches
        }
    };
}

// =============================================================================
// CACHE DEFINITIONS - Single source of truth
// =============================================================================
// To add a new cache:
// 1. Add the cache field to ApiCache in configuration.rs
// 2. Add the cache key constant to cache_keys.rs
// 3. Add an entry here: cache_keys::YOUR_KEY => your_field, TTL
// 4. Add a refresh_your_cache() function below
// 5. Add the match arm in refresh_single_cache()
// =============================================================================

define_caches! {
    // Metrics
    TVL => total_value_locked, CACHE_TTL_STANDARD,
    BORROWED_TOTAL => borrowed, CACHE_TTL_HOURLY,
    SUPPLIED_FUNDS => supplied_funds, CACHE_TTL_HOURLY,
    OPEN_INTEREST => open_interest, CACHE_TTL_HOURLY,
    OPEN_POSITION_VALUE => open_position_value, CACHE_TTL_HOURLY,

    // Treasury
    REVENUE => revenue, CACHE_TTL_STANDARD,
    BUYBACK_TOTAL => buyback_total, CACHE_TTL_STANDARD,
    DISTRIBUTED => distributed, CACHE_TTL_STANDARD,
    INCENTIVES_POOL => incentives_pool, CACHE_TTL_STANDARD,

    // PnL
    REALIZED_PNL_STATS => realized_pnl_stats, CACHE_TTL_STANDARD,
    REALIZED_PNL_WALLET => realized_pnl_wallet, CACHE_TTL_STANDARD,
    UNREALIZED_PNL => unrealized_pnl, CACHE_TTL_HOURLY,

    // Leases
    LEASES_MONTHLY => leases_monthly, CACHE_TTL_STANDARD,
    LEASE_VALUE_STATS => lease_value_stats, CACHE_TTL_HOURLY,
    LOANS_BY_TOKEN => loans_by_token, CACHE_TTL_HOURLY,
    LOANS_GRANTED => loans_granted, CACHE_TTL_STANDARD,
    LIQUIDATIONS => liquidations, CACHE_TTL_STANDARD,
    INTEREST_REPAYMENTS => interest_repayments, CACHE_TTL_STANDARD,
    HISTORICALLY_OPENED => historically_opened, CACHE_TTL_STANDARD,
    HISTORICALLY_REPAID => historically_repaid, CACHE_TTL_STANDARD,
    HISTORICALLY_LIQUIDATED => historically_liquidated, CACHE_TTL_STANDARD,

    // Positions
    POSITIONS => positions, CACHE_TTL_HOURLY,
    POSITION_BUCKETS => position_buckets, CACHE_TTL_HOURLY,
    OPEN_POSITIONS_BY_TOKEN => open_positions_by_token, CACHE_TTL_HOURLY,
    DAILY_POSITIONS => daily_positions, CACHE_TTL_HOURLY,

    // Liquidity
    POOLS => pools, CACHE_TTL_HOURLY,
    CURRENT_LENDERS => current_lenders, CACHE_TTL_HOURLY,
    HISTORICAL_LENDERS => historical_lenders, CACHE_TTL_STANDARD,
    // Misc
    TOTAL_TX_VALUE => total_tx_value, CACHE_TTL_STANDARD,
    MONTHLY_ACTIVE_WALLETS => monthly_active_wallets, CACHE_TTL_STANDARD,
    REVENUE_SERIES => revenue_series, CACHE_TTL_STANDARD,
}

// =============================================================================
// Main Task
// =============================================================================

/// Main background task for cache refresh
/// Runs indefinitely, checking and refreshing caches that are about to expire
pub async fn cache_refresh_task(
    app_state: AppState<State>,
) -> Result<(), Error> {
    let refresh_interval = app_state.config.cache_refresh_interval_secs;
    let max_concurrent = app_state.config.cache_max_concurrent_refreshes;
    let max_initial = app_state.config.cache_max_concurrent_initial_refreshes;

    let tracker = RefreshTracker::new();

    info!(
        "Starting cache refresh task (interval={}s, concurrent={}, initial={})",
        refresh_interval, max_concurrent, max_initial
    );

    // Initial population of all caches on startup (parallel)
    info!("Performing initial cache population...");
    if let Err(e) =
        refresh_all_caches_parallel(&app_state, &tracker, max_initial).await
    {
        error!("Error during initial cache population: {}", e);
    }
    info!("Initial cache population complete");

    // Main refresh loop
    let mut check_interval = interval(Duration::from_secs(refresh_interval));

    loop {
        check_interval.tick().await;

        if let Err(e) = check_and_refresh_caches_parallel(
            &app_state,
            &tracker,
            max_concurrent,
        )
        .await
        {
            error!("Error in cache refresh cycle: {}", e);
        }
    }
}

// =============================================================================
// Parallel Refresh Infrastructure
// =============================================================================

/// Check all caches and refresh those that need it (parallel with concurrency limit)
async fn check_and_refresh_caches_parallel(
    app_state: &AppState<State>,
    tracker: &RefreshTracker,
    max_concurrent: usize,
) -> Result<(), Error> {
    // Collect which caches need refresh
    let mut caches_to_refresh = Vec::new();

    for &cache_name in ALL_CACHE_NAMES {
        let ttl = ttl_for_cache(cache_name);
        if tracker.needs_refresh(cache_name, ttl).await {
            caches_to_refresh.push(cache_name);
        }
    }

    if caches_to_refresh.is_empty() {
        debug!("No caches need refresh");
        return Ok(());
    }

    debug!(
        "Refreshing {} caches in parallel (max {}): {:?}",
        caches_to_refresh.len(),
        max_concurrent,
        caches_to_refresh
    );

    // Refresh caches in parallel with concurrency limit
    let results: Vec<_> = stream::iter(caches_to_refresh)
        .map(|cache_name| {
            let app_state = app_state.clone();
            async move {
                let result = refresh_single_cache(&app_state, cache_name).await;
                (cache_name, result)
            }
        })
        .buffer_unordered(max_concurrent)
        .collect()
        .await;

    // Log results and record successful refreshes
    for (cache_name, result) in results {
        match result {
            Ok(()) => {
                tracker.record_refresh(cache_name).await;
                debug!("Successfully refreshed cache: {}", cache_name);
            },
            Err(e) => warn!("Failed to refresh cache {}: {}", cache_name, e),
        }
    }

    Ok(())
}

/// Refresh all caches on startup (parallel with concurrency limit)
async fn refresh_all_caches_parallel(
    app_state: &AppState<State>,
    tracker: &RefreshTracker,
    max_concurrent: usize,
) -> Result<(), Error> {
    info!(
        "Populating {} caches in parallel (max {})",
        ALL_CACHE_NAMES.len(),
        max_concurrent
    );

    // Refresh all caches in parallel with concurrency limit
    let results: Vec<_> = stream::iter(ALL_CACHE_NAMES.iter().copied())
        .map(|cache_name| {
            let app_state = app_state.clone();
            async move {
                info!("Initial population: {}", cache_name);
                let result = refresh_single_cache(&app_state, cache_name).await;
                (cache_name, result)
            }
        })
        .buffer_unordered(max_concurrent)
        .collect()
        .await;

    // Log results and record successful refreshes
    let mut success_count = 0;
    let mut failure_count = 0;
    for (cache_name, result) in results {
        match result {
            Ok(()) => {
                success_count += 1;
                tracker.record_refresh(cache_name).await;
            },
            Err(e) => {
                failure_count += 1;
                warn!(
                    "Failed to initially populate cache {}: {}",
                    cache_name, e
                );
            },
        }
    }

    info!(
        "Initial cache population complete: {} succeeded, {} failed",
        success_count, failure_count
    );

    Ok(())
}

// =============================================================================
// Refresh Dispatch
// =============================================================================

/// Refresh a single cache by name
async fn refresh_single_cache(
    app_state: &AppState<State>,
    cache_name: &str,
) -> Result<(), Error> {
    match cache_name {
        // Metrics
        cache_keys::TVL => refresh_tvl(app_state).await,
        cache_keys::BORROWED_TOTAL => refresh_borrowed(app_state).await,
        cache_keys::SUPPLIED_FUNDS => refresh_supplied_funds(app_state).await,
        cache_keys::OPEN_INTEREST => refresh_open_interest(app_state).await,
        cache_keys::OPEN_POSITION_VALUE => {
            refresh_open_position_value(app_state).await
        },

        // Treasury
        cache_keys::REVENUE => refresh_revenue(app_state).await,
        cache_keys::BUYBACK_TOTAL => refresh_buyback_total(app_state).await,
        cache_keys::DISTRIBUTED => refresh_distributed(app_state).await,
        cache_keys::INCENTIVES_POOL => refresh_incentives_pool(app_state).await,

        // PnL
        cache_keys::REALIZED_PNL_STATS => {
            refresh_realized_pnl_stats(app_state).await
        },
        cache_keys::REALIZED_PNL_WALLET => {
            refresh_realized_pnl_wallet(app_state).await
        },
        cache_keys::UNREALIZED_PNL => refresh_unrealized_pnl(app_state).await,

        // Leases
        cache_keys::LEASES_MONTHLY => refresh_leases_monthly(app_state).await,
        cache_keys::LEASE_VALUE_STATS => {
            refresh_lease_value_stats(app_state).await
        },
        cache_keys::LOANS_BY_TOKEN => refresh_loans_by_token(app_state).await,
        cache_keys::LOANS_GRANTED => refresh_loans_granted(app_state).await,
        cache_keys::LIQUIDATIONS => refresh_liquidations(app_state).await,
        cache_keys::INTEREST_REPAYMENTS => {
            refresh_interest_repayments(app_state).await
        },
        cache_keys::HISTORICALLY_OPENED => {
            refresh_historically_opened(app_state).await
        },
        cache_keys::HISTORICALLY_REPAID => {
            refresh_historically_repaid(app_state).await
        },
        cache_keys::HISTORICALLY_LIQUIDATED => {
            refresh_historically_liquidated(app_state).await
        },

        // Positions
        cache_keys::POSITIONS => refresh_positions(app_state).await,
        cache_keys::POSITION_BUCKETS => {
            refresh_position_buckets(app_state).await
        },
        cache_keys::OPEN_POSITIONS_BY_TOKEN => {
            refresh_open_positions_by_token(app_state).await
        },
        cache_keys::DAILY_POSITIONS => refresh_daily_positions(app_state).await,

        // Liquidity
        cache_keys::POOLS => refresh_pools(app_state).await,
        cache_keys::CURRENT_LENDERS => refresh_current_lenders(app_state).await,
        cache_keys::HISTORICAL_LENDERS => {
            refresh_historical_lenders(app_state).await
        },
        // Misc
        cache_keys::TOTAL_TX_VALUE => refresh_total_tx_value(app_state).await,
        cache_keys::MONTHLY_ACTIVE_WALLETS => {
            refresh_monthly_active_wallets(app_state).await
        },
        cache_keys::REVENUE_SERIES => refresh_revenue_series(app_state).await,

        _ => {
            warn!("Unknown cache name: {}", cache_name);
            Ok(())
        },
    }
}

// =============================================================================
// Individual Refresh Functions
// =============================================================================

async fn refresh_tvl(app_state: &AppState<State>) -> Result<(), Error> {
    let pool_ids = app_state.get_active_pool_ids();
    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(pool_ids)
        .await?;
    app_state
        .api_cache
        .total_value_locked
        .insert(cache_keys::TVL.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_revenue(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_revenue().await?;
    app_state
        .api_cache
        .revenue
        .insert(cache_keys::REVENUE.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_total_tx_value(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_total_tx_value().await?;
    app_state
        .api_cache
        .total_tx_value
        .insert(cache_keys::TOTAL_TX_VALUE.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_realized_pnl_stats(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_loan_closing
        .get_realized_pnl_stats()
        .await?
        + BigDecimal::from_str("2958250")?;
    let result = data.with_scale(2);
    app_state
        .api_cache
        .realized_pnl_stats
        .insert(cache_keys::REALIZED_PNL_STATS.to_string(), result)
        .await;
    Ok(())
}

async fn refresh_buyback_total(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_buyback_total().await?;
    app_state
        .api_cache
        .buyback_total
        .insert(cache_keys::BUYBACK_TOTAL.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_distributed(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state
        .database
        .tr_rewards_distribution
        .get_distributed()
        .await?;
    app_state
        .api_cache
        .distributed
        .insert(cache_keys::DISTRIBUTED.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_incentives_pool(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.tr_state.get_incentives_pool().await?;
    app_state
        .api_cache
        .incentives_pool
        .insert(cache_keys::INCENTIVES_POOL.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_open_position_value(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_state
        .get_open_position_value()
        .await?;
    app_state
        .api_cache
        .open_position_value
        .insert(cache_keys::OPEN_POSITION_VALUE.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_open_interest(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_interest().await?;
    app_state
        .api_cache
        .open_interest
        .insert(cache_keys::OPEN_INTEREST.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_supplied_funds(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .lp_pool_state
        .get_supplied_funds()
        .await?;
    let result = data.with_scale(2);
    app_state
        .api_cache
        .supplied_funds
        .insert(cache_keys::SUPPLIED_FUNDS.to_string(), result)
        .await;
    Ok(())
}

async fn refresh_unrealized_pnl(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_unrealized_pnl().await?;
    app_state
        .api_cache
        .unrealized_pnl
        .insert(cache_keys::UNREALIZED_PNL.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_leases_monthly(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_leases_monthly().await?;
    app_state
        .api_cache
        .leases_monthly
        .insert(cache_keys::LEASES_MONTHLY.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_monthly_active_wallets(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_opening
        .get_monthly_active_wallets()
        .await?;
    let wallets: Vec<MonthlyActiveWallet> = data
        .into_iter()
        .map(|w| MonthlyActiveWallet {
            month: w.month,
            unique_addresses: w.unique_addresses,
        })
        .collect();
    app_state
        .api_cache
        .monthly_active_wallets
        .insert(cache_keys::MONTHLY_ACTIVE_WALLETS.to_string(), wallets)
        .await;
    Ok(())
}

async fn refresh_revenue_series(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_revenue_series().await?;
    let series: Vec<RevenueSeriesPoint> = data
        .into_iter()
        .map(|(time, daily, cumulative)| RevenueSeriesPoint {
            time,
            daily,
            cumulative,
        })
        .collect();
    app_state
        .api_cache
        .revenue_series
        .insert(cache_keys::REVENUE_SERIES.to_string(), series)
        .await;
    Ok(())
}

async fn refresh_daily_positions(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_opening
        .get_daily_opened_closed_with_window(Some(3), None)
        .await?;
    let series: Vec<DailyPositionsPoint> = data
        .into_iter()
        .map(|(date, closed, opened)| DailyPositionsPoint {
            date,
            closed_loans: closed,
            opened_loans: opened,
        })
        .collect();
    app_state
        .api_cache
        .daily_positions
        .insert(cache_keys::DAILY_POSITIONS.to_string(), series)
        .await;
    Ok(())
}

async fn refresh_position_buckets(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_position_buckets().await?;
    let buckets: Vec<PositionBucket> = data
        .into_iter()
        .map(|b| PositionBucket {
            loan_category: b.loan_category.unwrap_or_default(),
            loan_count: b.loan_count,
            loan_size: b.loan_size,
        })
        .collect();
    app_state
        .api_cache
        .position_buckets
        .insert(cache_keys::POSITION_BUCKETS.to_string(), buckets)
        .await;
    Ok(())
}

async fn refresh_loans_by_token(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_loans_by_token().await?;
    let loans: Vec<TokenLoan> = data
        .into_iter()
        .map(|l| TokenLoan {
            symbol: l.symbol,
            value: l.value,
        })
        .collect();
    app_state
        .api_cache
        .loans_by_token
        .insert(cache_keys::LOANS_BY_TOKEN.to_string(), loans)
        .await;
    Ok(())
}

async fn refresh_open_positions_by_token(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_state
        .get_open_positions_by_token()
        .await?;
    let positions: Vec<TokenPosition> = data
        .into_iter()
        .map(|p| TokenPosition {
            token: p.token,
            market_value: p.market_value,
        })
        .collect();
    app_state
        .api_cache
        .open_positions_by_token
        .insert(cache_keys::OPEN_POSITIONS_BY_TOKEN.to_string(), positions)
        .await;
    Ok(())
}

async fn refresh_current_lenders(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .lp_lender_state
        .get_current_lenders()
        .await?;
    app_state
        .api_cache
        .current_lenders
        .insert(cache_keys::CURRENT_LENDERS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_lease_value_stats(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_lease_value_stats().await?;
    app_state
        .api_cache
        .lease_value_stats
        .insert(cache_keys::LEASE_VALUE_STATS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_loans_granted(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_loans_granted().await?;
    app_state
        .api_cache
        .loans_granted
        .insert(cache_keys::LOANS_GRANTED.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_historically_repaid(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_repayment
        .get_historically_repaid()
        .await?;
    app_state
        .api_cache
        .historically_repaid
        .insert(cache_keys::HISTORICALLY_REPAID.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_historically_liquidated(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_liquidation
        .get_historically_liquidated()
        .await?;
    app_state
        .api_cache
        .historically_liquidated
        .insert(cache_keys::HISTORICALLY_LIQUIDATED.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_positions(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_all_positions().await?;
    app_state
        .api_cache
        .positions
        .insert(cache_keys::POSITIONS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_liquidations(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_liquidation
        .get_all_liquidations()
        .await?;
    app_state
        .api_cache
        .liquidations
        .insert(cache_keys::LIQUIDATIONS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_historical_lenders(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .lp_deposit
        .get_all_historical_lenders()
        .await?;
    app_state
        .api_cache
        .historical_lenders
        .insert(cache_keys::HISTORICAL_LENDERS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_interest_repayments(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_repayment
        .get_interest_repayments_with_window(None, None)
        .await?;
    app_state
        .api_cache
        .interest_repayments
        .insert(cache_keys::INTEREST_REPAYMENTS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_historically_opened(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_opening
        .get_all_historically_opened()
        .await?;
    app_state
        .api_cache
        .historically_opened
        .insert(cache_keys::HISTORICALLY_OPENED.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_pools(app_state: &AppState<State>) -> Result<(), Error> {
    let data: Vec<PoolUtilizationLevel> = app_state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;
    app_state
        .api_cache
        .pools
        .insert(cache_keys::POOLS.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_realized_pnl_wallet(
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let data: Vec<RealizedPnlWallet> = app_state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(None, None)
        .await?;
    app_state
        .api_cache
        .realized_pnl_wallet
        .insert(cache_keys::REALIZED_PNL_WALLET.to_string(), data)
        .await;
    Ok(())
}

async fn refresh_borrowed(app_state: &AppState<State>) -> Result<(), Error> {
    // Fetch total and per-protocol borrowed in parallel (2 queries instead of 1 + N)
    let (total, borrowed_by_protocol) = tokio::try_join!(
        app_state.database.ls_opening.get_borrowed_total(),
        app_state.database.ls_opening.get_borrowed_by_protocols()
    )?;

    // Set total borrowed
    let total_key = build_protocol_cache_key("borrowed", None);
    app_state.api_cache.borrowed.insert(total_key, total).await;

    // Set per-protocol borrowed from the batch result
    for (protocol_key, protocol) in app_state.protocols.iter() {
        let cache_key =
            build_protocol_cache_key("borrowed", Some(protocol_key));
        let data = borrowed_by_protocol
            .get(&protocol.contracts.lpp)
            .cloned()
            .unwrap_or_else(|| BigDecimal::from_str("0").unwrap());
        app_state.api_cache.borrowed.insert(cache_key, data).await;
    }

    Ok(())
}
