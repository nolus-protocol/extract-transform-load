//! Treasury-related API endpoints
//!
//! Endpoints for revenue, buyback, distributed rewards, and incentives pool.

use actix_web::{get, web, HttpResponse, Responder};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    cache_keys,
    configuration::{AppState, State},
    error::Error,
    helpers::{
        build_cache_key, cached_fetch, parse_period_months, to_csv_response,
    },
    model::RevenueSeriesPoint,
};

// =============================================================================
// Revenue
// =============================================================================

#[get("/revenue")]
pub async fn revenue(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data =
        cached_fetch(&state.api_cache.revenue, cache_keys::REVENUE, || async {
            state.database.tr_profit.get_revenue().await
        })
        .await?;

    Ok(web::Json(RevenueResponse { revenue: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevenueResponse {
    pub revenue: BigDecimal,
}

// =============================================================================
// Revenue Series
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RevenueSeriesQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/revenue-series")]
pub async fn revenue_series(
    state: web::Data<AppState<State>>,
    query: web::Query<RevenueSeriesQuery>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("revenue_series", period_str, query.from);

    let fetch = || async {
        let data = state
            .database
            .tr_profit
            .get_revenue_series_with_window(months, query.from)
            .await?;
        let series: Vec<RevenueSeriesPoint> = data
            .into_iter()
            .map(|(time, daily, cumulative)| RevenueSeriesPoint {
                time,
                daily,
                cumulative,
            })
            .collect();
        Ok(series)
    };

    let data = if query.from.is_none() {
        cached_fetch(&state.api_cache.revenue_series, &cache_key, fetch).await?
    } else {
        fetch().await?
    };

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "revenue-series.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

// =============================================================================
// Distributed
// =============================================================================

#[get("/distributed")]
pub async fn distributed(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(
        &state.api_cache.distributed,
        cache_keys::DISTRIBUTED,
        || async {
            state
                .database
                .tr_rewards_distribution
                .get_distributed()
                .await
        },
    )
    .await?;

    Ok(web::Json(DistributedResponse { distributed: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DistributedResponse {
    pub distributed: BigDecimal,
}

// =============================================================================
// Buyback
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct BuybackQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/buyback")]
pub async fn buyback(
    state: web::Data<AppState<State>>,
    query: web::Query<BuybackQuery>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("buyback", period_str, query.from);

    let data = cached_fetch(&state.api_cache.buyback, &cache_key, || async {
        Ok(state
            .database
            .tr_profit
            .get_buyback_with_window(months, query.from)
            .await?)
    })
    .await?;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "buyback.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

// =============================================================================
// Buyback Total
// =============================================================================

#[get("/buyback-total")]
pub async fn buyback_total(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(
        &state.api_cache.buyback_total,
        cache_keys::BUYBACK_TOTAL,
        || async { state.database.tr_profit.get_buyback_total().await },
    )
    .await?;

    Ok(web::Json(BuybackTotalResponse {
        buyback_total: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BuybackTotalResponse {
    pub buyback_total: BigDecimal,
}

// =============================================================================
// Incentives Pool
// =============================================================================

#[get("/incentives-pool")]
pub async fn incentives_pool(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(
        &state.api_cache.incentives_pool,
        cache_keys::INCENTIVES_POOL,
        || async { state.database.tr_state.get_incentives_pool().await },
    )
    .await?;

    Ok(web::Json(IncentivesPoolResponse {
        incentives_pool: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IncentivesPoolResponse {
    pub incentives_pool: BigDecimal,
}

// =============================================================================
// Earnings
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct EarningsQuery {
    address: String,
}

#[get("/earnings")]
pub async fn earnings(
    state: web::Data<AppState<State>>,
    query: web::Query<EarningsQuery>,
) -> Result<impl Responder, Error> {
    let address = query.address.to_lowercase();
    let earnings = state.database.lp_pool_state.get_earnings(address).await?;
    Ok(web::Json(EarningsResponse { earnings }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EarningsResponse {
    pub earnings: BigDecimal,
}
