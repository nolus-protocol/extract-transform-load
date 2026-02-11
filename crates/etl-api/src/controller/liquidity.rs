//! Liquidity pool API endpoints
//!
//! Endpoints for pools, lenders, utilization, and LP operations.

use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use etl_core::{
    cache_keys,
    configuration::{AppState, State},
    dao::postgre::lp_pool_state::PoolUtilizationLevel,
    helpers::{build_cache_key, cached_fetch, parse_period_months},
};

use crate::csv_response::{to_csv_response, to_streaming_csv_response};

// =============================================================================
// Pools (batch endpoint)
// =============================================================================

#[derive(Debug, Serialize)]
pub struct PoolsResponse {
    pub protocols: Vec<PoolUtilizationLevel>,
    /// Optimal utilization rate threshold (percentage)
    pub optimal: String,
}

/// Batch endpoint to get pool data for all pools in a single request.
/// Returns utilization levels, supplied/borrowed amounts, and borrow APR.
#[get("/pools")]
pub async fn pools(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let data =
        cached_fetch(&state.api_cache.pools, cache_keys::POOLS, || async {
            Ok(state
                .database
                .lp_pool_state
                .get_all_utilization_levels()
                .await?)
        })
        .await?;

    Ok(HttpResponse::Ok().json(PoolsResponse {
        protocols: data,
        optimal: String::from("70.00"),
    }))
}

// =============================================================================
// LP Withdraw
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LpWithdrawQuery {
    tx: String,
}

#[get("/lp-withdraw")]
pub async fn lp_withdraw(
    state: web::Data<AppState<State>>,
    query: web::Query<LpWithdrawQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let tx = query.tx.to_owned();
    match state.database.lp_withdraw.get_by_tx(tx).await? {
        Some(data) => Ok(HttpResponse::Ok().json(data)),
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Transaction not found"
        }))),
    }
}

// =============================================================================
// Current Lenders
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct CurrentLendersQuery {
    format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Lender {
    pub joined: Option<DateTime<Utc>>,
    pub pool: Option<String>,
    pub lender: String,
    pub lent_stables: BigDecimal,
}

#[get("/current-lenders")]
pub async fn current_lenders(
    state: web::Data<AppState<State>>,
    query: web::Query<CurrentLendersQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.current_lenders,
        cache_keys::CURRENT_LENDERS,
        || async { state.database.lp_lender_state.get_current_lenders().await },
    )
    .await?;

    let lenders: Vec<Lender> = data
        .into_iter()
        .map(|l| Lender {
            joined: l.joined,
            pool: l.pool,
            lender: l.lender,
            lent_stables: l.lent_stables,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&lenders, "current-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(lenders)),
    }
}

// =============================================================================
// Historical Lenders
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoricalLendersQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}

impl From<etl_core::dao::postgre::lp_deposit::HistoricalLender>
    for HistoricalLender
{
    fn from(l: etl_core::dao::postgre::lp_deposit::HistoricalLender) -> Self {
        Self {
            transaction_type: l.transaction_type,
            timestamp: l.timestamp,
            user: l.user,
            amount: l.amount,
            pool: l.pool,
        }
    }
}

#[get("/historical-lenders")]
pub async fn historical_lenders(
    state: web::Data<AppState<State>>,
    query: web::Query<HistoricalLendersQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.historical_lenders,
            cache_keys::HISTORICAL_LENDERS,
            || async {
                state.database.lp_deposit.get_all_historical_lenders().await
            },
        )
        .await?;

        let response: Vec<HistoricalLender> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(response, "historical-lenders.csv");
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("historical_lenders", period_str, query.from);

    let data = cached_fetch(
        &state.api_cache.historical_lenders,
        &cache_key,
        || async {
            state
                .database
                .lp_deposit
                .get_historical_lenders_with_window(months, query.from)
                .await
        },
    )
    .await?;

    let response: Vec<HistoricalLender> =
        data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historical-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}
