//! Liquidity pool API endpoints
//!
//! Endpoints for pools, lenders, utilization, and LP operations.

use std::str::FromStr as _;

use actix_web::{get, web, HttpResponse, Responder};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    dao::postgre::lp_pool_state::PoolUtilizationLevel,
    error::Error,
    helpers::{build_cache_key, build_cache_key_with_protocol, parse_period_months, to_csv_response, to_streaming_csv_response},
};

// =============================================================================
// Pools (batch endpoint)
// =============================================================================

#[derive(Debug, Serialize)]
pub struct PoolsResponse {
    pub protocols: Vec<PoolUtilizationLevel>,
}

/// Batch endpoint to get pool data for all pools in a single request.
/// Returns utilization levels, supplied/borrowed amounts, and borrow APR.
#[get("/pools")]
pub async fn pools(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "pools_all";

    if let Some(cached) = state.api_cache.pools.get(CACHE_KEY).await {
        return Ok(HttpResponse::Ok().json(PoolsResponse {
            protocols: cached,
        }));
    }

    let data = state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;

    state.api_cache.pools.set(CACHE_KEY, data.clone()).await;

    Ok(HttpResponse::Ok().json(PoolsResponse { protocols: data }))
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
) -> Result<HttpResponse, Error> {
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
) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "current_lenders";

    if let Some(cached) = state.api_cache.current_lenders.get(CACHE_KEY).await {
        let lenders: Vec<Lender> = cached
            .into_iter()
            .map(|l| Lender {
                joined: l.joined,
                pool: l.pool,
                lender: l.lender,
                lent_stables: l.lent_stables,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&lenders, "current-lenders.csv"),
            _ => Ok(HttpResponse::Ok().json(lenders)),
        };
    }

    let data = state.database.lp_lender_state.get_current_lenders().await?;

    state.api_cache.current_lenders.set(CACHE_KEY, data.clone()).await;

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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}

impl From<crate::dao::postgre::lp_deposit::HistoricalLender> for HistoricalLender {
    fn from(l: crate::dao::postgre::lp_deposit::HistoricalLender) -> Self {
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
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("historical_lenders", period_str, query.from);

    if let Some(cached) = state.api_cache.historical_lenders.get(&cache_key).await {
        let data: Vec<HistoricalLender> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "historical-lenders.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .lp_deposit
        .get_historical_lenders_with_window(months, query.from)
        .await?;

    state.api_cache.historical_lenders.set(&cache_key, data.clone()).await;

    let response: Vec<HistoricalLender> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historical-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[get("/historical-lenders/export")]
pub async fn historical_lenders_export(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "historical_lenders_all";

    if let Some(cached) = state.api_cache.historical_lenders.get(CACHE_KEY).await {
        let data: Vec<HistoricalLender> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "historical-lenders.csv");
    }

    let data = state.database.lp_deposit.get_all_historical_lenders().await?;
    state.api_cache.historical_lenders.set(CACHE_KEY, data.clone()).await;

    let response: Vec<HistoricalLender> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "historical-lenders.csv")
}

// =============================================================================
// Utilization Level
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct UtilizationLevelQuery {
    format: Option<String>,
    period: Option<String>,
    protocol: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/utilization-level")]
pub async fn utilization_level(
    state: web::Data<AppState<State>>,
    query: web::Query<UtilizationLevelQuery>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");

    if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        let admin = state.protocols.get(&protocol_key);

        if let Some(protocol) = admin {
            let cache_key = build_cache_key_with_protocol("utilization_level", &protocol_key, period_str, query.from);

            if let Some(cached) = state.api_cache.utilization_level.get(&cache_key).await {
                let items: Vec<BigDecimal> = cached
                    .iter()
                    .map(|item| item.utilization_level.clone())
                    .collect();
                return match query.format.as_deref() {
                    Some("csv") => to_csv_response(&cached, "utilization-level.csv"),
                    _ => Ok(HttpResponse::Ok().json(items)),
                };
            }

            let data = state
                .database
                .lp_pool_state
                .get_utilization_level_with_window(protocol.contracts.lpp.clone(), months, query.from)
                .await?;

            state.api_cache.utilization_level.set(&cache_key, data.clone()).await;

            let items: Vec<BigDecimal> = data
                .iter()
                .map(|item| item.utilization_level.clone())
                .collect();

            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&data, "utilization-level.csv"),
                _ => Ok(HttpResponse::Ok().json(items)),
            };
        }
    }

    let items: Vec<BigDecimal> = vec![];
    Ok(HttpResponse::Ok().json(items))
}

// =============================================================================
// Deposit Suspension
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct DepositSuspensionResponse {
    pub deposit_suspension: String,
}

#[get("/deposit-suspension")]
pub async fn deposit_suspension() -> Result<impl Responder, Error> {
    Ok(web::Json(DepositSuspensionResponse {
        deposit_suspension: String::from("65.00"),
    }))
}

// =============================================================================
// Earn APR (deprecated)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct EarnAprQuery {
    protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EarnAprResponse {
    pub earn_apr: BigDecimal,
}

/// DEPRECATED: Use /api/pools endpoint instead, which includes earn_apr for all pools.
#[get("/earn-apr")]
pub async fn earn_apr(
    state: web::Data<AppState<State>>,
    query: web::Query<EarnAprQuery>,
) -> Result<HttpResponse, Error> {
    let earn_apr = if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        let admin = state.protocols.get(&protocol_key);
        if let Some(protocol) = admin {
            match protocol_key.as_str() {
                "OSMOSIS-OSMOSIS-ALL_BTC" | "OSMOSIS-OSMOSIS-ATOM" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        2.5,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                "OSMOSIS-OSMOSIS-ALL_SOL" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        4.0,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                "OSMOSIS-OSMOSIS-ST_ATOM" | "OSMOSIS-OSMOSIS-AKT" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        2.0,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                _ => state
                    .database
                    .ls_opening
                    .get_earn_apr(protocol.contracts.lpp.to_owned())
                    .await
                    .unwrap_or(BigDecimal::from(0)),
            }
        } else {
            BigDecimal::from_str("0")?
        }
    } else {
        BigDecimal::from_str("0")?
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Deprecation", "true"))
        .insert_header(("Sunset", "2025-06-01"))
        .insert_header(("Link", "</api/pools>; rel=\"successor-version\""))
        .json(EarnAprResponse { earn_apr }))
}
