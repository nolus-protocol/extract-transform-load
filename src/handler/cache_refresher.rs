//! Background cache refresh task
//!
//! This module provides proactive cache refresh for API endpoints.
//! It runs in the background, checking which caches are about to expire
//! and refreshing them with controlled concurrency to avoid DB load spikes.

use std::str::FromStr;
use std::time::Duration;

use bigdecimal::BigDecimal;
use futures::stream::{self, StreamExt};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::{
    cache_keys,
    configuration::{AppState, State},
    dao::postgre::{lp_pool_state::PoolUtilizationLevel, ls_opening::RealizedPnlWallet},
    error::Error,
    helpers::{build_cache_key_with_protocol, build_protocol_cache_key},
    model::{
        DailyPositionsPoint, MonthlyActiveWallet, PositionBucket,
        RevenueSeriesPoint, TokenLoan, TokenPosition,
    },
};

/// How often to check for caches needing refresh (in seconds)
const REFRESH_CHECK_INTERVAL_SECS: u64 = 30;

/// Maximum number of concurrent cache refreshes
/// This limits DB load while still being faster than sequential
const MAX_CONCURRENT_REFRESHES: usize = 4;

/// Maximum number of concurrent refreshes during initial population
/// Higher than normal since we want startup to be fast
const MAX_CONCURRENT_INITIAL_REFRESHES: usize = 6;

/// All cache names for iteration
const ALL_CACHE_NAMES: &[&str] = &[
    cache_keys::TVL,
    cache_keys::REVENUE,
    cache_keys::TOTAL_TX_VALUE,
    cache_keys::REALIZED_PNL_STATS,
    cache_keys::CURRENT_LENDERS,
    cache_keys::LEASE_VALUE_STATS,
    cache_keys::DAILY_POSITIONS,
    cache_keys::POSITION_BUCKETS,
    cache_keys::LOANS_BY_TOKEN,
    cache_keys::OPEN_POSITIONS_BY_TOKEN,
    cache_keys::LOANS_GRANTED,
    cache_keys::HISTORICALLY_REPAID,
    cache_keys::HISTORICALLY_LIQUIDATED,
    cache_keys::BUYBACK_TOTAL,
    cache_keys::DISTRIBUTED,
    cache_keys::INCENTIVES_POOL,
    cache_keys::OPEN_POSITION_VALUE,
    cache_keys::OPEN_INTEREST,
    cache_keys::SUPPLIED_FUNDS,
    cache_keys::UNREALIZED_PNL,
    cache_keys::LEASES_MONTHLY,
    cache_keys::MONTHLY_ACTIVE_WALLETS,
    cache_keys::REVENUE_SERIES,
    cache_keys::POSITIONS,
    cache_keys::LIQUIDATIONS,
    cache_keys::HISTORICAL_LENDERS,
    cache_keys::INTEREST_REPAYMENTS,
    cache_keys::HISTORICALLY_OPENED,
    cache_keys::POOLS,
    cache_keys::REALIZED_PNL_WALLET,
    cache_keys::BORROWED_TOTAL,
    cache_keys::UTILIZATION_LEVEL_PROTOCOL,
];

/// Main background task for cache refresh
/// Runs indefinitely, checking and refreshing caches that are about to expire
pub async fn cache_refresh_task(app_state: AppState<State>) -> Result<(), Error> {
    info!("Starting cache refresh background task");

    // Initial population of all caches on startup (parallel)
    info!("Performing initial cache population...");
    if let Err(e) = refresh_all_caches_parallel(&app_state).await {
        error!("Error during initial cache population: {}", e);
    }
    info!("Initial cache population complete");

    // Main refresh loop
    let mut check_interval = interval(Duration::from_secs(REFRESH_CHECK_INTERVAL_SECS));

    loop {
        check_interval.tick().await;

        if let Err(e) = check_and_refresh_caches_parallel(&app_state).await {
            error!("Error in cache refresh cycle: {}", e);
        }
    }
}

/// Check if a cache needs refresh by name
async fn cache_needs_refresh(app_state: &AppState<State>, cache_name: &str) -> bool {
    let cache = &app_state.api_cache;
    match cache_name {
        cache_keys::TVL => cache.total_value_locked.needs_refresh(cache_keys::TVL).await,
        cache_keys::REVENUE => cache.revenue.needs_refresh(cache_keys::REVENUE).await,
        cache_keys::TOTAL_TX_VALUE => cache.total_tx_value.needs_refresh(cache_keys::TOTAL_TX_VALUE).await,
        cache_keys::REALIZED_PNL_STATS => cache.realized_pnl_stats.needs_refresh(cache_keys::REALIZED_PNL_STATS).await,
        cache_keys::CURRENT_LENDERS => cache.current_lenders.needs_refresh(cache_keys::CURRENT_LENDERS).await,
        cache_keys::LEASE_VALUE_STATS => cache.lease_value_stats.needs_refresh(cache_keys::LEASE_VALUE_STATS).await,
        cache_keys::DAILY_POSITIONS => cache.daily_positions.needs_refresh(cache_keys::DAILY_POSITIONS).await,
        cache_keys::POSITION_BUCKETS => cache.position_buckets.needs_refresh(cache_keys::POSITION_BUCKETS).await,
        cache_keys::LOANS_BY_TOKEN => cache.loans_by_token.needs_refresh(cache_keys::LOANS_BY_TOKEN).await,
        cache_keys::OPEN_POSITIONS_BY_TOKEN => cache.open_positions_by_token.needs_refresh(cache_keys::OPEN_POSITIONS_BY_TOKEN).await,
        cache_keys::LOANS_GRANTED => cache.loans_granted.needs_refresh(cache_keys::LOANS_GRANTED).await,
        cache_keys::HISTORICALLY_REPAID => cache.historically_repaid.needs_refresh(cache_keys::HISTORICALLY_REPAID).await,
        cache_keys::HISTORICALLY_LIQUIDATED => cache.historically_liquidated.needs_refresh(cache_keys::HISTORICALLY_LIQUIDATED).await,
        cache_keys::BUYBACK_TOTAL => cache.buyback_total.needs_refresh(cache_keys::BUYBACK_TOTAL).await,
        cache_keys::DISTRIBUTED => cache.distributed.needs_refresh(cache_keys::DISTRIBUTED).await,
        cache_keys::INCENTIVES_POOL => cache.incentives_pool.needs_refresh(cache_keys::INCENTIVES_POOL).await,
        cache_keys::OPEN_POSITION_VALUE => cache.open_position_value.needs_refresh(cache_keys::OPEN_POSITION_VALUE).await,
        cache_keys::OPEN_INTEREST => cache.open_interest.needs_refresh(cache_keys::OPEN_INTEREST).await,
        cache_keys::SUPPLIED_FUNDS => cache.supplied_funds.needs_refresh(cache_keys::SUPPLIED_FUNDS).await,
        cache_keys::UNREALIZED_PNL => cache.unrealized_pnl.needs_refresh(cache_keys::UNREALIZED_PNL).await,
        cache_keys::LEASES_MONTHLY => cache.leases_monthly.needs_refresh(cache_keys::LEASES_MONTHLY).await,
        cache_keys::MONTHLY_ACTIVE_WALLETS => cache.monthly_active_wallets.needs_refresh(cache_keys::MONTHLY_ACTIVE_WALLETS).await,
        cache_keys::REVENUE_SERIES => cache.revenue_series.needs_refresh(cache_keys::REVENUE_SERIES).await,
        cache_keys::POSITIONS => cache.positions.needs_refresh(cache_keys::POSITIONS).await,
        cache_keys::LIQUIDATIONS => cache.liquidations.needs_refresh(cache_keys::LIQUIDATIONS).await,
        cache_keys::HISTORICAL_LENDERS => cache.historical_lenders.needs_refresh(cache_keys::HISTORICAL_LENDERS).await,
        cache_keys::INTEREST_REPAYMENTS => cache.interest_repayments.needs_refresh(cache_keys::INTEREST_REPAYMENTS).await,
        cache_keys::HISTORICALLY_OPENED => cache.historically_opened.needs_refresh(cache_keys::HISTORICALLY_OPENED).await,
        cache_keys::POOLS => cache.pools.needs_refresh(cache_keys::POOLS).await,
        cache_keys::REALIZED_PNL_WALLET => cache.realized_pnl_wallet.needs_refresh(cache_keys::REALIZED_PNL_WALLET).await,
        cache_keys::BORROWED_TOTAL => cache.borrowed.needs_refresh(cache_keys::BORROWED_TOTAL).await,
        cache_keys::UTILIZATION_LEVEL_PROTOCOL => cache.utilization_level.needs_refresh("utilization_level_OSMOSIS_3m_none").await,
        _ => false,
    }
}

/// Check all caches and refresh those that need it (parallel with concurrency limit)
async fn check_and_refresh_caches_parallel(app_state: &AppState<State>) -> Result<(), Error> {
    // Collect which caches need refresh
    let mut caches_to_refresh = Vec::new();

    for &cache_name in ALL_CACHE_NAMES {
        if cache_needs_refresh(app_state, cache_name).await {
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
        MAX_CONCURRENT_REFRESHES,
        caches_to_refresh
    );

    // Refresh caches in parallel with concurrency limit
    let results: Vec<_> = stream::iter(caches_to_refresh)
        .map(|cache_name| {
            let app_state = app_state.clone();
            async move {
                let result = refresh_single_cache_with_lock(&app_state, cache_name).await;
                (cache_name, result)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_REFRESHES)
        .collect()
        .await;

    // Log results
    for (cache_name, result) in results {
        match result {
            Ok(true) => debug!("Successfully refreshed cache: {}", cache_name),
            Ok(false) => debug!("Cache {} already being refreshed, skipped", cache_name),
            Err(e) => warn!("Failed to refresh cache {}: {}", cache_name, e),
        }
    }

    Ok(())
}

/// Refresh all caches on startup (parallel with concurrency limit)
async fn refresh_all_caches_parallel(app_state: &AppState<State>) -> Result<(), Error> {
    info!(
        "Populating {} caches in parallel (max {})",
        ALL_CACHE_NAMES.len(),
        MAX_CONCURRENT_INITIAL_REFRESHES
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
        .buffer_unordered(MAX_CONCURRENT_INITIAL_REFRESHES)
        .collect()
        .await;

    // Log results
    let mut success_count = 0;
    let mut failure_count = 0;
    for (cache_name, result) in results {
        match result {
            Ok(()) => success_count += 1,
            Err(e) => {
                failure_count += 1;
                warn!("Failed to initially populate cache {}: {}", cache_name, e);
            }
        }
    }

    info!(
        "Initial cache population complete: {} succeeded, {} failed",
        success_count, failure_count
    );

    Ok(())
}

/// Refresh a single cache with stampede protection.
/// Returns Ok(true) if refresh was performed, Ok(false) if already refreshing.
async fn refresh_single_cache_with_lock(
    app_state: &AppState<State>,
    cache_name: &str,
) -> Result<bool, Error> {
    // Get the appropriate cache and try to acquire refresh lock
    let acquired = match cache_name {
        cache_keys::TVL => app_state.api_cache.total_value_locked.try_start_refresh(cache_keys::TVL).await,
        cache_keys::REVENUE => app_state.api_cache.revenue.try_start_refresh(cache_keys::REVENUE).await,
        cache_keys::TOTAL_TX_VALUE => app_state.api_cache.total_tx_value.try_start_refresh(cache_keys::TOTAL_TX_VALUE).await,
        cache_keys::REALIZED_PNL_STATS => app_state.api_cache.realized_pnl_stats.try_start_refresh(cache_keys::REALIZED_PNL_STATS).await,
        cache_keys::CURRENT_LENDERS => app_state.api_cache.current_lenders.try_start_refresh(cache_keys::CURRENT_LENDERS).await,
        cache_keys::LEASE_VALUE_STATS => app_state.api_cache.lease_value_stats.try_start_refresh(cache_keys::LEASE_VALUE_STATS).await,
        cache_keys::DAILY_POSITIONS => app_state.api_cache.daily_positions.try_start_refresh(cache_keys::DAILY_POSITIONS).await,
        cache_keys::POSITION_BUCKETS => app_state.api_cache.position_buckets.try_start_refresh(cache_keys::POSITION_BUCKETS).await,
        cache_keys::LOANS_BY_TOKEN => app_state.api_cache.loans_by_token.try_start_refresh(cache_keys::LOANS_BY_TOKEN).await,
        cache_keys::OPEN_POSITIONS_BY_TOKEN => app_state.api_cache.open_positions_by_token.try_start_refresh(cache_keys::OPEN_POSITIONS_BY_TOKEN).await,
        cache_keys::LOANS_GRANTED => app_state.api_cache.loans_granted.try_start_refresh(cache_keys::LOANS_GRANTED).await,
        cache_keys::HISTORICALLY_REPAID => app_state.api_cache.historically_repaid.try_start_refresh(cache_keys::HISTORICALLY_REPAID).await,
        cache_keys::HISTORICALLY_LIQUIDATED => app_state.api_cache.historically_liquidated.try_start_refresh(cache_keys::HISTORICALLY_LIQUIDATED).await,
        cache_keys::BUYBACK_TOTAL => app_state.api_cache.buyback_total.try_start_refresh(cache_keys::BUYBACK_TOTAL).await,
        cache_keys::DISTRIBUTED => app_state.api_cache.distributed.try_start_refresh(cache_keys::DISTRIBUTED).await,
        cache_keys::INCENTIVES_POOL => app_state.api_cache.incentives_pool.try_start_refresh(cache_keys::INCENTIVES_POOL).await,
        cache_keys::OPEN_POSITION_VALUE => app_state.api_cache.open_position_value.try_start_refresh(cache_keys::OPEN_POSITION_VALUE).await,
        cache_keys::OPEN_INTEREST => app_state.api_cache.open_interest.try_start_refresh(cache_keys::OPEN_INTEREST).await,
        cache_keys::SUPPLIED_FUNDS => app_state.api_cache.supplied_funds.try_start_refresh(cache_keys::SUPPLIED_FUNDS).await,
        cache_keys::UNREALIZED_PNL => app_state.api_cache.unrealized_pnl.try_start_refresh(cache_keys::UNREALIZED_PNL).await,
        cache_keys::LEASES_MONTHLY => app_state.api_cache.leases_monthly.try_start_refresh(cache_keys::LEASES_MONTHLY).await,
        cache_keys::MONTHLY_ACTIVE_WALLETS => app_state.api_cache.monthly_active_wallets.try_start_refresh(cache_keys::MONTHLY_ACTIVE_WALLETS).await,
        cache_keys::REVENUE_SERIES => app_state.api_cache.revenue_series.try_start_refresh(cache_keys::REVENUE_SERIES).await,
        cache_keys::POSITIONS => app_state.api_cache.positions.try_start_refresh(cache_keys::POSITIONS).await,
        cache_keys::LIQUIDATIONS => app_state.api_cache.liquidations.try_start_refresh(cache_keys::LIQUIDATIONS).await,
        cache_keys::HISTORICAL_LENDERS => app_state.api_cache.historical_lenders.try_start_refresh(cache_keys::HISTORICAL_LENDERS).await,
        cache_keys::INTEREST_REPAYMENTS => app_state.api_cache.interest_repayments.try_start_refresh(cache_keys::INTEREST_REPAYMENTS).await,
        cache_keys::HISTORICALLY_OPENED => app_state.api_cache.historically_opened.try_start_refresh(cache_keys::HISTORICALLY_OPENED).await,
        cache_keys::POOLS => app_state.api_cache.pools.try_start_refresh(cache_keys::POOLS).await,
        cache_keys::REALIZED_PNL_WALLET => app_state.api_cache.realized_pnl_wallet.try_start_refresh(cache_keys::REALIZED_PNL_WALLET).await,
        cache_keys::BORROWED_TOTAL => app_state.api_cache.borrowed.try_start_refresh(cache_keys::BORROWED_TOTAL).await,
        cache_keys::UTILIZATION_LEVEL_PROTOCOL => app_state.api_cache.utilization_level.try_start_refresh("utilization_level_OSMOSIS_3m_none").await,
        _ => {
            warn!("Unknown cache name: {}", cache_name);
            return Ok(false);
        }
    };

    if !acquired {
        // Another task is already refreshing this cache
        return Ok(false);
    }

    // Do the actual refresh
    let result = refresh_single_cache(app_state, cache_name).await;

    // Release the lock
    match cache_name {
        cache_keys::TVL => app_state.api_cache.total_value_locked.complete_refresh(cache_keys::TVL).await,
        cache_keys::REVENUE => app_state.api_cache.revenue.complete_refresh(cache_keys::REVENUE).await,
        cache_keys::TOTAL_TX_VALUE => app_state.api_cache.total_tx_value.complete_refresh(cache_keys::TOTAL_TX_VALUE).await,
        cache_keys::REALIZED_PNL_STATS => app_state.api_cache.realized_pnl_stats.complete_refresh(cache_keys::REALIZED_PNL_STATS).await,
        cache_keys::CURRENT_LENDERS => app_state.api_cache.current_lenders.complete_refresh(cache_keys::CURRENT_LENDERS).await,
        cache_keys::LEASE_VALUE_STATS => app_state.api_cache.lease_value_stats.complete_refresh(cache_keys::LEASE_VALUE_STATS).await,
        cache_keys::DAILY_POSITIONS => app_state.api_cache.daily_positions.complete_refresh(cache_keys::DAILY_POSITIONS).await,
        cache_keys::POSITION_BUCKETS => app_state.api_cache.position_buckets.complete_refresh(cache_keys::POSITION_BUCKETS).await,
        cache_keys::LOANS_BY_TOKEN => app_state.api_cache.loans_by_token.complete_refresh(cache_keys::LOANS_BY_TOKEN).await,
        cache_keys::OPEN_POSITIONS_BY_TOKEN => app_state.api_cache.open_positions_by_token.complete_refresh(cache_keys::OPEN_POSITIONS_BY_TOKEN).await,
        cache_keys::LOANS_GRANTED => app_state.api_cache.loans_granted.complete_refresh(cache_keys::LOANS_GRANTED).await,
        cache_keys::HISTORICALLY_REPAID => app_state.api_cache.historically_repaid.complete_refresh(cache_keys::HISTORICALLY_REPAID).await,
        cache_keys::HISTORICALLY_LIQUIDATED => app_state.api_cache.historically_liquidated.complete_refresh(cache_keys::HISTORICALLY_LIQUIDATED).await,
        cache_keys::BUYBACK_TOTAL => app_state.api_cache.buyback_total.complete_refresh(cache_keys::BUYBACK_TOTAL).await,
        cache_keys::DISTRIBUTED => app_state.api_cache.distributed.complete_refresh(cache_keys::DISTRIBUTED).await,
        cache_keys::INCENTIVES_POOL => app_state.api_cache.incentives_pool.complete_refresh(cache_keys::INCENTIVES_POOL).await,
        cache_keys::OPEN_POSITION_VALUE => app_state.api_cache.open_position_value.complete_refresh(cache_keys::OPEN_POSITION_VALUE).await,
        cache_keys::OPEN_INTEREST => app_state.api_cache.open_interest.complete_refresh(cache_keys::OPEN_INTEREST).await,
        cache_keys::SUPPLIED_FUNDS => app_state.api_cache.supplied_funds.complete_refresh(cache_keys::SUPPLIED_FUNDS).await,
        cache_keys::UNREALIZED_PNL => app_state.api_cache.unrealized_pnl.complete_refresh(cache_keys::UNREALIZED_PNL).await,
        cache_keys::LEASES_MONTHLY => app_state.api_cache.leases_monthly.complete_refresh(cache_keys::LEASES_MONTHLY).await,
        cache_keys::MONTHLY_ACTIVE_WALLETS => app_state.api_cache.monthly_active_wallets.complete_refresh(cache_keys::MONTHLY_ACTIVE_WALLETS).await,
        cache_keys::REVENUE_SERIES => app_state.api_cache.revenue_series.complete_refresh(cache_keys::REVENUE_SERIES).await,
        cache_keys::POSITIONS => app_state.api_cache.positions.complete_refresh(cache_keys::POSITIONS).await,
        cache_keys::LIQUIDATIONS => app_state.api_cache.liquidations.complete_refresh(cache_keys::LIQUIDATIONS).await,
        cache_keys::HISTORICAL_LENDERS => app_state.api_cache.historical_lenders.complete_refresh(cache_keys::HISTORICAL_LENDERS).await,
        cache_keys::INTEREST_REPAYMENTS => app_state.api_cache.interest_repayments.complete_refresh(cache_keys::INTEREST_REPAYMENTS).await,
        cache_keys::HISTORICALLY_OPENED => app_state.api_cache.historically_opened.complete_refresh(cache_keys::HISTORICALLY_OPENED).await,
        cache_keys::POOLS => app_state.api_cache.pools.complete_refresh(cache_keys::POOLS).await,
        cache_keys::REALIZED_PNL_WALLET => app_state.api_cache.realized_pnl_wallet.complete_refresh(cache_keys::REALIZED_PNL_WALLET).await,
        cache_keys::BORROWED_TOTAL => app_state.api_cache.borrowed.complete_refresh(cache_keys::BORROWED_TOTAL).await,
        cache_keys::UTILIZATION_LEVEL_PROTOCOL => app_state.api_cache.utilization_level.complete_refresh("utilization_level_OSMOSIS_3m_none").await,
        _ => {}
    };

    result.map(|()| true)
}

/// Refresh a single cache by name (without lock - used for initial population)
async fn refresh_single_cache(app_state: &AppState<State>, cache_name: &str) -> Result<(), Error> {
    match cache_name {
        cache_keys::TVL => refresh_tvl(app_state).await,
        cache_keys::REVENUE => refresh_revenue(app_state).await,
        cache_keys::TOTAL_TX_VALUE => refresh_total_tx_value(app_state).await,
        cache_keys::REALIZED_PNL_STATS => refresh_realized_pnl_stats(app_state).await,
        cache_keys::BUYBACK_TOTAL => refresh_buyback_total(app_state).await,
        cache_keys::DISTRIBUTED => refresh_distributed(app_state).await,
        cache_keys::INCENTIVES_POOL => refresh_incentives_pool(app_state).await,
        cache_keys::OPEN_POSITION_VALUE => refresh_open_position_value(app_state).await,
        cache_keys::OPEN_INTEREST => refresh_open_interest(app_state).await,
        cache_keys::SUPPLIED_FUNDS => refresh_supplied_funds(app_state).await,
        cache_keys::UNREALIZED_PNL => refresh_unrealized_pnl(app_state).await,
        cache_keys::LEASES_MONTHLY => refresh_leases_monthly(app_state).await,
        cache_keys::MONTHLY_ACTIVE_WALLETS => refresh_monthly_active_wallets(app_state).await,
        cache_keys::REVENUE_SERIES => refresh_revenue_series(app_state).await,
        cache_keys::DAILY_POSITIONS => refresh_daily_positions(app_state).await,
        cache_keys::POSITION_BUCKETS => refresh_position_buckets(app_state).await,
        cache_keys::LOANS_BY_TOKEN => refresh_loans_by_token(app_state).await,
        cache_keys::OPEN_POSITIONS_BY_TOKEN => refresh_open_positions_by_token(app_state).await,
        cache_keys::CURRENT_LENDERS => refresh_current_lenders(app_state).await,
        cache_keys::LEASE_VALUE_STATS => refresh_lease_value_stats(app_state).await,
        cache_keys::LOANS_GRANTED => refresh_loans_granted(app_state).await,
        cache_keys::HISTORICALLY_REPAID => refresh_historically_repaid(app_state).await,
        cache_keys::HISTORICALLY_LIQUIDATED => refresh_historically_liquidated(app_state).await,
        cache_keys::POSITIONS => refresh_positions(app_state).await,
        cache_keys::LIQUIDATIONS => refresh_liquidations(app_state).await,
        cache_keys::HISTORICAL_LENDERS => refresh_historical_lenders(app_state).await,
        cache_keys::INTEREST_REPAYMENTS => refresh_interest_repayments(app_state).await,
        cache_keys::HISTORICALLY_OPENED => refresh_historically_opened(app_state).await,
        cache_keys::POOLS => refresh_pools(app_state).await,
        cache_keys::REALIZED_PNL_WALLET => refresh_realized_pnl_wallet(app_state).await,
        cache_keys::BORROWED_TOTAL => refresh_borrowed(app_state).await,
        cache_keys::UTILIZATION_LEVEL_PROTOCOL => refresh_utilization_level_protocols(app_state).await,
        _ => {
            warn!("Unknown cache name: {}", cache_name);
            Ok(())
        }
    }
}

// Public function for external callers (e.g., aggregation_task)
pub async fn refresh_tvl_cache(app_state: &AppState<State>) -> Result<(), Error> {
    refresh_tvl(app_state).await
}

// Individual refresh functions for each cache

async fn refresh_tvl(app_state: &AppState<State>) -> Result<(), Error> {
    let tvl_params = app_state.build_tvl_pool_params();
    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(tvl_params)
        .await?;
    app_state.api_cache.total_value_locked.set(cache_keys::TVL, data).await;
    Ok(())
}

async fn refresh_revenue(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_revenue().await?;
    app_state.api_cache.revenue.set(cache_keys::REVENUE, data).await;
    Ok(())
}

async fn refresh_total_tx_value(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_total_tx_value().await?;
    app_state.api_cache.total_tx_value.set(cache_keys::TOTAL_TX_VALUE, data).await;
    Ok(())
}

async fn refresh_realized_pnl_stats(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_loan_closing
        .get_realized_pnl_stats()
        .await?
        + BigDecimal::from_str("2958250")?;
    let result = data.with_scale(2);
    app_state.api_cache.realized_pnl_stats.set(cache_keys::REALIZED_PNL_STATS, result).await;
    Ok(())
}

async fn refresh_buyback_total(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_buyback_total().await?;
    app_state.api_cache.buyback_total.set(cache_keys::BUYBACK_TOTAL, data).await;
    Ok(())
}

async fn refresh_distributed(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_rewards_distribution.get_distributed().await?;
    app_state.api_cache.distributed.set(cache_keys::DISTRIBUTED, data).await;
    Ok(())
}

async fn refresh_incentives_pool(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_state.get_incentives_pool().await?;
    app_state.api_cache.incentives_pool.set(cache_keys::INCENTIVES_POOL, data).await;
    Ok(())
}

async fn refresh_open_position_value(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_position_value().await?;
    app_state.api_cache.open_position_value.set(cache_keys::OPEN_POSITION_VALUE, data).await;
    Ok(())
}

async fn refresh_open_interest(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_interest().await?;
    app_state.api_cache.open_interest.set(cache_keys::OPEN_INTEREST, data).await;
    Ok(())
}

async fn refresh_supplied_funds(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_pool_state.get_supplied_funds().await?;
    let result = data.with_scale(2);
    app_state.api_cache.supplied_funds.set(cache_keys::SUPPLIED_FUNDS, result).await;
    Ok(())
}

async fn refresh_unrealized_pnl(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_unrealized_pnl().await?;
    app_state.api_cache.unrealized_pnl.set(cache_keys::UNREALIZED_PNL, data).await;
    Ok(())
}

async fn refresh_leases_monthly(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_leases_monthly().await?;
    app_state.api_cache.leases_monthly.set(cache_keys::LEASES_MONTHLY, data).await;
    Ok(())
}

async fn refresh_monthly_active_wallets(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_monthly_active_wallets().await?;
    let wallets: Vec<MonthlyActiveWallet> = data
        .into_iter()
        .map(|w| MonthlyActiveWallet {
            month: w.month,
            unique_addresses: w.unique_addresses,
        })
        .collect();
    app_state.api_cache.monthly_active_wallets.set(cache_keys::MONTHLY_ACTIVE_WALLETS, wallets).await;
    Ok(())
}

async fn refresh_revenue_series(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_revenue_series().await?;
    let series: Vec<RevenueSeriesPoint> = data
        .into_iter()
        .map(|(time, daily, cumulative)| RevenueSeriesPoint {
            time,
            daily,
            cumulative,
        })
        .collect();
    app_state.api_cache.revenue_series.set(cache_keys::REVENUE_SERIES, series).await;
    Ok(())
}

async fn refresh_daily_positions(app_state: &AppState<State>) -> Result<(), Error> {
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
    app_state.api_cache.daily_positions.set(cache_keys::DAILY_POSITIONS, series).await;
    Ok(())
}

async fn refresh_position_buckets(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_position_buckets().await?;
    let buckets: Vec<PositionBucket> = data
        .into_iter()
        .map(|b| PositionBucket {
            loan_category: b.loan_category.unwrap_or_default(),
            loan_count: b.loan_count,
            loan_size: b.loan_size,
        })
        .collect();
    app_state.api_cache.position_buckets.set(cache_keys::POSITION_BUCKETS, buckets).await;
    Ok(())
}

async fn refresh_loans_by_token(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_loans_by_token().await?;
    let loans: Vec<TokenLoan> = data
        .into_iter()
        .map(|l| TokenLoan {
            symbol: l.symbol,
            value: l.value,
        })
        .collect();
    app_state.api_cache.loans_by_token.set(cache_keys::LOANS_BY_TOKEN, loans).await;
    Ok(())
}

async fn refresh_open_positions_by_token(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_positions_by_token().await?;
    let positions: Vec<TokenPosition> = data
        .into_iter()
        .map(|p| TokenPosition {
            token: p.token,
            market_value: p.market_value,
        })
        .collect();
    app_state.api_cache.open_positions_by_token.set(cache_keys::OPEN_POSITIONS_BY_TOKEN, positions).await;
    Ok(())
}

async fn refresh_current_lenders(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_lender_state.get_current_lenders().await?;
    app_state.api_cache.current_lenders.set(cache_keys::CURRENT_LENDERS, data).await;
    Ok(())
}

async fn refresh_lease_value_stats(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_lease_value_stats().await?;
    app_state.api_cache.lease_value_stats.set(cache_keys::LEASE_VALUE_STATS, data).await;
    Ok(())
}

async fn refresh_loans_granted(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_loans_granted().await?;
    app_state.api_cache.loans_granted.set(cache_keys::LOANS_GRANTED, data).await;
    Ok(())
}

async fn refresh_historically_repaid(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_repayment.get_historically_repaid().await?;
    app_state.api_cache.historically_repaid.set(cache_keys::HISTORICALLY_REPAID, data).await;
    Ok(())
}

async fn refresh_historically_liquidated(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_liquidation.get_historically_liquidated().await?;
    app_state.api_cache.historically_liquidated.set(cache_keys::HISTORICALLY_LIQUIDATED, data).await;
    Ok(())
}

async fn refresh_positions(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_all_positions().await?;
    app_state.api_cache.positions.set(cache_keys::POSITIONS, data).await;
    Ok(())
}

async fn refresh_liquidations(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_liquidation.get_all_liquidations().await?;
    app_state.api_cache.liquidations.set(cache_keys::LIQUIDATIONS, data).await;
    Ok(())
}

async fn refresh_historical_lenders(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_deposit.get_all_historical_lenders().await?;
    app_state.api_cache.historical_lenders.set(cache_keys::HISTORICAL_LENDERS, data).await;
    Ok(())
}

async fn refresh_interest_repayments(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_repayment
        .get_interest_repayments_with_window(None, None)
        .await?;
    app_state.api_cache.interest_repayments.set(cache_keys::INTEREST_REPAYMENTS, data).await;
    Ok(())
}

async fn refresh_historically_opened(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_all_historically_opened().await?;
    app_state.api_cache.historically_opened.set(cache_keys::HISTORICALLY_OPENED, data).await;
    Ok(())
}

async fn refresh_pools(app_state: &AppState<State>) -> Result<(), Error> {
    let data: Vec<PoolUtilizationLevel> = app_state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;
    app_state.api_cache.pools.set(cache_keys::POOLS, data).await;
    Ok(())
}

async fn refresh_realized_pnl_wallet(app_state: &AppState<State>) -> Result<(), Error> {
    let data: Vec<RealizedPnlWallet> = app_state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(None, None)
        .await?;
    app_state.api_cache.realized_pnl_wallet.set(cache_keys::REALIZED_PNL_WALLET, data).await;
    Ok(())
}

async fn refresh_borrowed(app_state: &AppState<State>) -> Result<(), Error> {
    // Refresh total borrowed
    let total = app_state.database.ls_opening.get_borrowed_total().await?;
    let total_key = build_protocol_cache_key("borrowed", None);
    app_state.api_cache.borrowed.set(&total_key, total).await;

    // Refresh per-protocol borrowed
    for (protocol_key, protocol) in app_state.protocols.iter() {
        let cache_key = build_protocol_cache_key("borrowed", Some(protocol_key));
        let data = app_state
            .database
            .ls_opening
            .get_borrowed(protocol.contracts.lpp.clone())
            .await?;
        app_state.api_cache.borrowed.set(&cache_key, data).await;
    }

    Ok(())
}

async fn refresh_utilization_level_protocols(app_state: &AppState<State>) -> Result<(), Error> {
    for (protocol_key, protocol) in app_state.protocols.iter() {
        let cache_key = build_cache_key_with_protocol("utilization_level", protocol_key, "3m", None);
        let data = app_state
            .database
            .lp_pool_state
            .get_utilization_level_with_window(protocol.contracts.lpp.clone(), Some(3), None)
            .await?;
        app_state.api_cache.utilization_level.set(&cache_key, data).await;
    }

    Ok(())
}
