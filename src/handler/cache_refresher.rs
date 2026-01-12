//! Background cache refresh task
//!
//! This module provides proactive cache refresh for API endpoints.
//! It runs in the background, checking which caches are about to expire
//! and refreshing them one at a time to avoid DB load spikes.

use std::str::FromStr;
use std::time::Duration;

use bigdecimal::BigDecimal;
use tokio::time::{self, interval};
use tracing::{debug, error, info, warn};

use crate::{
    configuration::{AppState, State},
    dao::postgre::{lp_pool_state::PoolUtilizationLevel, ls_opening::RealizedPnlWallet},
    error::Error,
    helpers::{build_cache_key_with_protocol, build_protocol_cache_key},
    model::{
        DailyPositionsPoint, MonthlyActiveWallet, PositionBucket,
        RevenueSeriesPoint, TokenLoan, TokenPosition, TvlPoolParams,
    },
};

/// How often to check for caches needing refresh (in seconds)
const REFRESH_CHECK_INTERVAL_SECS: u64 = 30;

/// Delay between individual cache refreshes (in seconds)
/// This spreads DB load over time
const DELAY_BETWEEN_REFRESHES_SECS: u64 = 2;

/// Cache key constants matching those used in controllers
const CACHE_KEY_TVL: &str = "tvl";
const CACHE_KEY_REVENUE: &str = "revenue";
const CACHE_KEY_TOTAL_TX_VALUE: &str = "total_tx_value";
const CACHE_KEY_REALIZED_PNL_STATS: &str = "realized_pnl_stats";
const CACHE_KEY_BUYBACK_TOTAL: &str = "buyback_total";
const CACHE_KEY_DISTRIBUTED: &str = "distributed";
const CACHE_KEY_INCENTIVES_POOL: &str = "incentives_pool";
const CACHE_KEY_OPEN_POSITION_VALUE: &str = "open_position_value";
const CACHE_KEY_OPEN_INTEREST: &str = "open_interest";
const CACHE_KEY_SUPPLIED_FUNDS: &str = "supplied_funds";
const CACHE_KEY_UNREALIZED_PNL: &str = "unrealized_pnl";
const CACHE_KEY_LEASES_MONTHLY: &str = "leases_monthly";
const CACHE_KEY_MONTHLY_ACTIVE_WALLETS: &str = "monthly_active_wallets";
const CACHE_KEY_REVENUE_SERIES: &str = "revenue_series";
const CACHE_KEY_DAILY_POSITIONS: &str = "daily_positions_3m_none";
const CACHE_KEY_POSITION_BUCKETS: &str = "position_buckets";
const CACHE_KEY_LOANS_BY_TOKEN: &str = "loans_by_token";
const CACHE_KEY_OPEN_POSITIONS_BY_TOKEN: &str = "open_positions_by_token";
const CACHE_KEY_CURRENT_LENDERS: &str = "current_lenders";
const CACHE_KEY_LEASE_VALUE_STATS: &str = "lease_value_stats";
const CACHE_KEY_LOANS_GRANTED: &str = "loans_granted";
const CACHE_KEY_HISTORICALLY_REPAID: &str = "historically_repaid";
const CACHE_KEY_HISTORICALLY_LIQUIDATED: &str = "historically_liquidated";
const CACHE_KEY_POSITIONS: &str = "positions_all";
const CACHE_KEY_LIQUIDATIONS: &str = "liquidations_all";
const CACHE_KEY_HISTORICAL_LENDERS: &str = "historical_lenders_all";
const CACHE_KEY_INTEREST_REPAYMENTS: &str = "interest_repayments_all";
const CACHE_KEY_HISTORICALLY_OPENED: &str = "historically_opened_all";
const CACHE_KEY_POOLS: &str = "pools_all";
const CACHE_KEY_REALIZED_PNL_WALLET: &str = "realized_pnl_wallet_all";
const CACHE_KEY_BORROWED_TOTAL: &str = "borrowed_total";
const CACHE_KEY_UTILIZATION_LEVEL_PROTOCOL: &str = "utilization_level_protocol";

/// Main background task for cache refresh
/// Runs indefinitely, checking and refreshing caches that are about to expire
pub async fn cache_refresh_task(app_state: AppState<State>) -> Result<(), Error> {
    info!("Starting cache refresh background task");

    // Initial population of all caches on startup
    info!("Performing initial cache population...");
    if let Err(e) = refresh_all_caches(&app_state).await {
        error!("Error during initial cache population: {}", e);
    }
    info!("Initial cache population complete");

    // Main refresh loop
    let mut check_interval = interval(Duration::from_secs(REFRESH_CHECK_INTERVAL_SECS));

    loop {
        check_interval.tick().await;

        if let Err(e) = check_and_refresh_caches(&app_state).await {
            error!("Error in cache refresh cycle: {}", e);
        }
    }
}

/// Check all caches and refresh those that need it
async fn check_and_refresh_caches(app_state: &AppState<State>) -> Result<(), Error> {
    let cache = &app_state.api_cache;

    // List of all Type A caches to check, in priority order
    let checks: Vec<(&str, bool)> = vec![
        // 30-minute TTL caches (higher priority)
        (CACHE_KEY_TVL, cache.total_value_locked.needs_refresh(CACHE_KEY_TVL).await),
        (CACHE_KEY_REVENUE, cache.revenue.needs_refresh(CACHE_KEY_REVENUE).await),
        (CACHE_KEY_TOTAL_TX_VALUE, cache.total_tx_value.needs_refresh(CACHE_KEY_TOTAL_TX_VALUE).await),
        (CACHE_KEY_REALIZED_PNL_STATS, cache.realized_pnl_stats.needs_refresh(CACHE_KEY_REALIZED_PNL_STATS).await),
        (CACHE_KEY_CURRENT_LENDERS, cache.current_lenders.needs_refresh(CACHE_KEY_CURRENT_LENDERS).await),
        (CACHE_KEY_LEASE_VALUE_STATS, cache.lease_value_stats.needs_refresh(CACHE_KEY_LEASE_VALUE_STATS).await),
        (CACHE_KEY_DAILY_POSITIONS, cache.daily_positions.needs_refresh(CACHE_KEY_DAILY_POSITIONS).await),
        (CACHE_KEY_POSITION_BUCKETS, cache.position_buckets.needs_refresh(CACHE_KEY_POSITION_BUCKETS).await),
        (CACHE_KEY_LOANS_BY_TOKEN, cache.loans_by_token.needs_refresh(CACHE_KEY_LOANS_BY_TOKEN).await),
        (CACHE_KEY_OPEN_POSITIONS_BY_TOKEN, cache.open_positions_by_token.needs_refresh(CACHE_KEY_OPEN_POSITIONS_BY_TOKEN).await),
        (CACHE_KEY_LOANS_GRANTED, cache.loans_granted.needs_refresh(CACHE_KEY_LOANS_GRANTED).await),
        (CACHE_KEY_HISTORICALLY_REPAID, cache.historically_repaid.needs_refresh(CACHE_KEY_HISTORICALLY_REPAID).await),
        (CACHE_KEY_HISTORICALLY_LIQUIDATED, cache.historically_liquidated.needs_refresh(CACHE_KEY_HISTORICALLY_LIQUIDATED).await),
        // 1-hour TTL caches (lower priority)
        (CACHE_KEY_BUYBACK_TOTAL, cache.buyback_total.needs_refresh(CACHE_KEY_BUYBACK_TOTAL).await),
        (CACHE_KEY_DISTRIBUTED, cache.distributed.needs_refresh(CACHE_KEY_DISTRIBUTED).await),
        (CACHE_KEY_INCENTIVES_POOL, cache.incentives_pool.needs_refresh(CACHE_KEY_INCENTIVES_POOL).await),
        (CACHE_KEY_OPEN_POSITION_VALUE, cache.open_position_value.needs_refresh(CACHE_KEY_OPEN_POSITION_VALUE).await),
        (CACHE_KEY_OPEN_INTEREST, cache.open_interest.needs_refresh(CACHE_KEY_OPEN_INTEREST).await),
        (CACHE_KEY_SUPPLIED_FUNDS, cache.supplied_funds.needs_refresh(CACHE_KEY_SUPPLIED_FUNDS).await),
        (CACHE_KEY_UNREALIZED_PNL, cache.unrealized_pnl.needs_refresh(CACHE_KEY_UNREALIZED_PNL).await),
        (CACHE_KEY_LEASES_MONTHLY, cache.leases_monthly.needs_refresh(CACHE_KEY_LEASES_MONTHLY).await),
        (CACHE_KEY_MONTHLY_ACTIVE_WALLETS, cache.monthly_active_wallets.needs_refresh(CACHE_KEY_MONTHLY_ACTIVE_WALLETS).await),
        (CACHE_KEY_REVENUE_SERIES, cache.revenue_series.needs_refresh(CACHE_KEY_REVENUE_SERIES).await),
        // Previously lazy-only caches now proactively refreshed
        (CACHE_KEY_POSITIONS, cache.positions.needs_refresh(CACHE_KEY_POSITIONS).await),
        (CACHE_KEY_LIQUIDATIONS, cache.liquidations.needs_refresh(CACHE_KEY_LIQUIDATIONS).await),
        (CACHE_KEY_HISTORICAL_LENDERS, cache.historical_lenders.needs_refresh(CACHE_KEY_HISTORICAL_LENDERS).await),
        (CACHE_KEY_INTEREST_REPAYMENTS, cache.interest_repayments.needs_refresh(CACHE_KEY_INTEREST_REPAYMENTS).await),
        (CACHE_KEY_HISTORICALLY_OPENED, cache.historically_opened.needs_refresh(CACHE_KEY_HISTORICALLY_OPENED).await),
        (CACHE_KEY_POOLS, cache.pools.needs_refresh(CACHE_KEY_POOLS).await),
        (CACHE_KEY_REALIZED_PNL_WALLET, cache.realized_pnl_wallet.needs_refresh(CACHE_KEY_REALIZED_PNL_WALLET).await),
        // Protocol-specific caches (total only - per-protocol refreshed separately)
        (CACHE_KEY_BORROWED_TOTAL, cache.borrowed.needs_refresh(CACHE_KEY_BORROWED_TOTAL).await),
        (CACHE_KEY_UTILIZATION_LEVEL_PROTOCOL, cache.utilization_level.needs_refresh("utilization_level_OSMOSIS_3m_none").await),
    ];

    // Refresh caches that need it, one at a time with delay
    for (cache_name, needs_refresh) in checks {
        if needs_refresh {
            debug!("Refreshing cache: {}", cache_name);
            if let Err(e) = refresh_single_cache(app_state, cache_name).await {
                warn!("Failed to refresh cache {}: {}", cache_name, e);
            } else {
                debug!("Successfully refreshed cache: {}", cache_name);
            }
            // Delay before next refresh to spread DB load
            time::sleep(Duration::from_secs(DELAY_BETWEEN_REFRESHES_SECS)).await;
        }
    }

    Ok(())
}

/// Refresh all caches (used on startup)
async fn refresh_all_caches(app_state: &AppState<State>) -> Result<(), Error> {
    let all_caches = vec![
        CACHE_KEY_TVL,
        CACHE_KEY_REVENUE,
        CACHE_KEY_TOTAL_TX_VALUE,
        CACHE_KEY_REALIZED_PNL_STATS,
        CACHE_KEY_CURRENT_LENDERS,
        CACHE_KEY_LEASE_VALUE_STATS,
        CACHE_KEY_DAILY_POSITIONS,
        CACHE_KEY_POSITION_BUCKETS,
        CACHE_KEY_LOANS_BY_TOKEN,
        CACHE_KEY_OPEN_POSITIONS_BY_TOKEN,
        CACHE_KEY_LOANS_GRANTED,
        CACHE_KEY_HISTORICALLY_REPAID,
        CACHE_KEY_HISTORICALLY_LIQUIDATED,
        CACHE_KEY_BUYBACK_TOTAL,
        CACHE_KEY_DISTRIBUTED,
        CACHE_KEY_INCENTIVES_POOL,
        CACHE_KEY_OPEN_POSITION_VALUE,
        CACHE_KEY_OPEN_INTEREST,
        CACHE_KEY_SUPPLIED_FUNDS,
        CACHE_KEY_UNREALIZED_PNL,
        CACHE_KEY_LEASES_MONTHLY,
        CACHE_KEY_MONTHLY_ACTIVE_WALLETS,
        CACHE_KEY_REVENUE_SERIES,
        CACHE_KEY_POSITIONS,
        CACHE_KEY_LIQUIDATIONS,
        CACHE_KEY_HISTORICAL_LENDERS,
        CACHE_KEY_INTEREST_REPAYMENTS,
        CACHE_KEY_HISTORICALLY_OPENED,
        CACHE_KEY_POOLS,
        CACHE_KEY_REALIZED_PNL_WALLET,
        CACHE_KEY_BORROWED_TOTAL,
        CACHE_KEY_UTILIZATION_LEVEL_PROTOCOL,
    ];

    for cache_name in all_caches {
        info!("Initial population: {}", cache_name);
        if let Err(e) = refresh_single_cache(app_state, cache_name).await {
            warn!("Failed to initially populate cache {}: {}", cache_name, e);
        }
        // Small delay between initial loads
        time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

/// Refresh a single cache by name
async fn refresh_single_cache(app_state: &AppState<State>, cache_name: &str) -> Result<(), Error> {
    match cache_name {
        CACHE_KEY_TVL => refresh_tvl(app_state).await,
        CACHE_KEY_REVENUE => refresh_revenue(app_state).await,
        CACHE_KEY_TOTAL_TX_VALUE => refresh_total_tx_value(app_state).await,
        CACHE_KEY_REALIZED_PNL_STATS => refresh_realized_pnl_stats(app_state).await,
        CACHE_KEY_BUYBACK_TOTAL => refresh_buyback_total(app_state).await,
        CACHE_KEY_DISTRIBUTED => refresh_distributed(app_state).await,
        CACHE_KEY_INCENTIVES_POOL => refresh_incentives_pool(app_state).await,
        CACHE_KEY_OPEN_POSITION_VALUE => refresh_open_position_value(app_state).await,
        CACHE_KEY_OPEN_INTEREST => refresh_open_interest(app_state).await,
        CACHE_KEY_SUPPLIED_FUNDS => refresh_supplied_funds(app_state).await,
        CACHE_KEY_UNREALIZED_PNL => refresh_unrealized_pnl(app_state).await,
        CACHE_KEY_LEASES_MONTHLY => refresh_leases_monthly(app_state).await,
        CACHE_KEY_MONTHLY_ACTIVE_WALLETS => refresh_monthly_active_wallets(app_state).await,
        CACHE_KEY_REVENUE_SERIES => refresh_revenue_series(app_state).await,
        CACHE_KEY_DAILY_POSITIONS => refresh_daily_positions(app_state).await,
        CACHE_KEY_POSITION_BUCKETS => refresh_position_buckets(app_state).await,
        CACHE_KEY_LOANS_BY_TOKEN => refresh_loans_by_token(app_state).await,
        CACHE_KEY_OPEN_POSITIONS_BY_TOKEN => refresh_open_positions_by_token(app_state).await,
        CACHE_KEY_CURRENT_LENDERS => refresh_current_lenders(app_state).await,
        CACHE_KEY_LEASE_VALUE_STATS => refresh_lease_value_stats(app_state).await,
        CACHE_KEY_LOANS_GRANTED => refresh_loans_granted(app_state).await,
        CACHE_KEY_HISTORICALLY_REPAID => refresh_historically_repaid(app_state).await,
        CACHE_KEY_HISTORICALLY_LIQUIDATED => refresh_historically_liquidated(app_state).await,
        CACHE_KEY_POSITIONS => refresh_positions(app_state).await,
        CACHE_KEY_LIQUIDATIONS => refresh_liquidations(app_state).await,
        CACHE_KEY_HISTORICAL_LENDERS => refresh_historical_lenders(app_state).await,
        CACHE_KEY_INTEREST_REPAYMENTS => refresh_interest_repayments(app_state).await,
        CACHE_KEY_HISTORICALLY_OPENED => refresh_historically_opened(app_state).await,
        CACHE_KEY_POOLS => refresh_pools(app_state).await,
        CACHE_KEY_REALIZED_PNL_WALLET => refresh_realized_pnl_wallet(app_state).await,
        CACHE_KEY_BORROWED_TOTAL => refresh_borrowed(app_state).await,
        CACHE_KEY_UTILIZATION_LEVEL_PROTOCOL => refresh_utilization_level_protocols(app_state).await,
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
    let pools = &app_state.config.lp_pools;

    let get_pool = |idx: usize, name: &str| -> Result<String, Error> {
        pools
            .get(idx)
            .map(|(id, _, _, _)| id.clone())
            .ok_or_else(|| Error::ProtocolError(name.to_string()))
    };

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(TvlPoolParams {
            osmosis_usdc: get_pool(1, "osmosis_usdc")?,
            neutron_axelar: get_pool(2, "neutron_usdc_axelar")?,
            osmosis_usdc_noble: get_pool(3, "osmosis_usdc_noble")?,
            neutron_usdc_noble: get_pool(0, "neutron_usdc_noble")?,
            osmosis_st_atom: get_pool(4, "osmosis_st_atom")?,
            osmosis_all_btc: get_pool(5, "osmosis_all_btc")?,
            osmosis_all_sol: get_pool(6, "osmosis_all_sol")?,
            osmosis_akt: get_pool(7, "osmosis_akt")?,
        })
        .await?;

    app_state.api_cache.total_value_locked.set(CACHE_KEY_TVL, data).await;
    Ok(())
}

async fn refresh_revenue(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_revenue().await?;
    app_state.api_cache.revenue.set(CACHE_KEY_REVENUE, data).await;
    Ok(())
}

async fn refresh_total_tx_value(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_total_tx_value().await?;
    app_state.api_cache.total_tx_value.set(CACHE_KEY_TOTAL_TX_VALUE, data).await;
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
    app_state.api_cache.realized_pnl_stats.set(CACHE_KEY_REALIZED_PNL_STATS, result).await;
    Ok(())
}

async fn refresh_buyback_total(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_profit.get_buyback_total().await?;
    app_state.api_cache.buyback_total.set(CACHE_KEY_BUYBACK_TOTAL, data).await;
    Ok(())
}

async fn refresh_distributed(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_rewards_distribution.get_distributed().await?;
    app_state.api_cache.distributed.set(CACHE_KEY_DISTRIBUTED, data).await;
    Ok(())
}

async fn refresh_incentives_pool(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.tr_state.get_incentives_pool().await?;
    app_state.api_cache.incentives_pool.set(CACHE_KEY_INCENTIVES_POOL, data).await;
    Ok(())
}

async fn refresh_open_position_value(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_position_value().await?;
    app_state.api_cache.open_position_value.set(CACHE_KEY_OPEN_POSITION_VALUE, data).await;
    Ok(())
}

async fn refresh_open_interest(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_open_interest().await?;
    app_state.api_cache.open_interest.set(CACHE_KEY_OPEN_INTEREST, data).await;
    Ok(())
}

async fn refresh_supplied_funds(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_pool_state.get_supplied_funds().await?;
    let result = data.with_scale(2);
    app_state.api_cache.supplied_funds.set(CACHE_KEY_SUPPLIED_FUNDS, result).await;
    Ok(())
}

async fn refresh_unrealized_pnl(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_unrealized_pnl().await?;
    app_state.api_cache.unrealized_pnl.set(CACHE_KEY_UNREALIZED_PNL, data).await;
    Ok(())
}

async fn refresh_leases_monthly(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_leases_monthly().await?;
    app_state.api_cache.leases_monthly.set(CACHE_KEY_LEASES_MONTHLY, data).await;
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
    app_state.api_cache.monthly_active_wallets.set(CACHE_KEY_MONTHLY_ACTIVE_WALLETS, wallets).await;
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
    app_state.api_cache.revenue_series.set(CACHE_KEY_REVENUE_SERIES, series).await;
    Ok(())
}

async fn refresh_daily_positions(app_state: &AppState<State>) -> Result<(), Error> {
    // Refresh with default 3m period
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
    app_state.api_cache.daily_positions.set(CACHE_KEY_DAILY_POSITIONS, series).await;
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
    app_state.api_cache.position_buckets.set(CACHE_KEY_POSITION_BUCKETS, buckets).await;
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
    app_state.api_cache.loans_by_token.set(CACHE_KEY_LOANS_BY_TOKEN, loans).await;
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
    app_state.api_cache.open_positions_by_token.set(CACHE_KEY_OPEN_POSITIONS_BY_TOKEN, positions).await;
    Ok(())
}

async fn refresh_current_lenders(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_lender_state.get_current_lenders().await?;
    app_state.api_cache.current_lenders.set(CACHE_KEY_CURRENT_LENDERS, data).await;
    Ok(())
}

async fn refresh_lease_value_stats(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_lease_value_stats().await?;
    app_state.api_cache.lease_value_stats.set(CACHE_KEY_LEASE_VALUE_STATS, data).await;
    Ok(())
}

async fn refresh_loans_granted(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_loans_granted().await?;
    app_state.api_cache.loans_granted.set(CACHE_KEY_LOANS_GRANTED, data).await;
    Ok(())
}

async fn refresh_historically_repaid(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_repayment.get_historically_repaid().await?;
    app_state.api_cache.historically_repaid.set(CACHE_KEY_HISTORICALLY_REPAID, data).await;
    Ok(())
}

async fn refresh_historically_liquidated(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_liquidation.get_historically_liquidated().await?;
    app_state.api_cache.historically_liquidated.set(CACHE_KEY_HISTORICALLY_LIQUIDATED, data).await;
    Ok(())
}

async fn refresh_positions(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_all_positions().await?;
    app_state.api_cache.positions.set(CACHE_KEY_POSITIONS, data).await;
    Ok(())
}

async fn refresh_liquidations(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_liquidation.get_all_liquidations().await?;
    app_state.api_cache.liquidations.set(CACHE_KEY_LIQUIDATIONS, data).await;
    Ok(())
}

async fn refresh_historical_lenders(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_deposit.get_all_historical_lenders().await?;
    app_state.api_cache.historical_lenders.set(CACHE_KEY_HISTORICAL_LENDERS, data).await;
    Ok(())
}

async fn refresh_interest_repayments(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state
        .database
        .ls_repayment
        .get_interest_repayments_with_window(None, None)
        .await?;
    app_state.api_cache.interest_repayments.set(CACHE_KEY_INTEREST_REPAYMENTS, data).await;
    Ok(())
}

async fn refresh_historically_opened(app_state: &AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_opening.get_all_historically_opened().await?;
    app_state.api_cache.historically_opened.set(CACHE_KEY_HISTORICALLY_OPENED, data).await;
    Ok(())
}

async fn refresh_pools(app_state: &AppState<State>) -> Result<(), Error> {
    let data: Vec<PoolUtilizationLevel> = app_state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;
    app_state.api_cache.pools.set(CACHE_KEY_POOLS, data).await;
    Ok(())
}

async fn refresh_realized_pnl_wallet(app_state: &AppState<State>) -> Result<(), Error> {
    let data: Vec<RealizedPnlWallet> = app_state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(None, None)
        .await?;
    app_state.api_cache.realized_pnl_wallet.set(CACHE_KEY_REALIZED_PNL_WALLET, data).await;
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
    // Refresh per-protocol utilization levels with default 3m period
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
