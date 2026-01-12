//! Profit & Loss API endpoints
//!
//! Endpoints for realized and unrealized PnL, per-wallet data, and time series.

use std::str::FromStr;

use actix_web::{get, web, HttpResponse, Responder};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, cached_fetch, parse_period_months, to_csv_response, to_streaming_csv_response},
};

// =============================================================================
// Realized PnL (by address)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RealizedPnlQuery {
    address: String,
}

#[get("/realized-pnl")]
pub async fn realized_pnl(
    state: web::Data<AppState<State>>,
    query: web::Query<RealizedPnlQuery>,
) -> Result<impl Responder, Error> {
    let address = query.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_loan_closing
        .get_realized_pnl(address)
        .await?;
    Ok(web::Json(RealizedPnlResponse { realized_pnl: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RealizedPnlResponse {
    pub realized_pnl: f64,
}

// =============================================================================
// Realized PnL Data (by address)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RealizedPnlDataQuery {
    address: String,
}

#[get("/realized-pnl-data")]
pub async fn realized_pnl_data(
    state: web::Data<AppState<State>>,
    query: web::Query<RealizedPnlDataQuery>,
) -> Result<impl Responder, Error> {
    let address = query.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_opening
        .get_realized_pnl_data(address)
        .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Realized PnL Stats (platform-wide)
// =============================================================================

#[get("/realized-pnl-stats")]
pub async fn realized_pnl_stats(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.realized_pnl_stats, "realized_pnl_stats", || async {
        let data = state.database.ls_loan_closing.get_realized_pnl_stats().await?
            + BigDecimal::from_str("2958250")?;
        Ok(data.with_scale(2))
    })
    .await?;

    Ok(web::Json(RealizedPnlStatsResponse { amount: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RealizedPnlStatsResponse {
    pub amount: BigDecimal,
}

// =============================================================================
// Realized PnL by Wallet
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RealizedPnlWalletQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/realized-pnl-wallet")]
pub async fn realized_pnl_wallet(
    state: web::Data<AppState<State>>,
    query: web::Query<RealizedPnlWalletQuery>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("realized_pnl_wallet", period_str, query.from);

    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "realized-pnl-wallet.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    let data = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(months, query.from)
        .await?;

    state.api_cache.realized_pnl_wallet.set(&cache_key, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "realized-pnl-wallet.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

#[get("/realized-pnl-wallet/export")]
pub async fn realized_pnl_wallet_export(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "realized_pnl_wallet_all";

    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(CACHE_KEY).await {
        return to_streaming_csv_response(cached, "realized-pnl-wallet.csv");
    }

    let data = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(None, None)
        .await?;

    state.api_cache.realized_pnl_wallet.set(CACHE_KEY, data.clone()).await;

    to_streaming_csv_response(data, "realized-pnl-wallet.csv")
}

// =============================================================================
// Unrealized PnL (platform-wide)
// =============================================================================

#[get("/unrealized-pnl")]
pub async fn unrealized_pnl(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.unrealized_pnl, "unrealized_pnl", || async {
        state.database.ls_state.get_unrealized_pnl().await
    })
    .await?;

    Ok(web::Json(UnrealizedPnlResponse { unrealized_pnl: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnrealizedPnlResponse {
    pub unrealized_pnl: BigDecimal,
}

// =============================================================================
// Unrealized PnL by Address
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct UnrealizedPnlByAddressQuery {
    address: String,
}

#[get("/unrealized-pnl-by-address")]
pub async fn unrealized_pnl_by_address(
    state: web::Data<AppState<State>>,
    query: web::Query<UnrealizedPnlByAddressQuery>,
) -> Result<impl Responder, Error> {
    let items = state
        .database
        .ls_state
        .get_unrealized_pnl_by_address(query.address.to_owned())
        .await?;

    Ok(web::Json(items))
}

// =============================================================================
// PnL Over Time
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PnlOverTimeQuery {
    interval: i64,
    address: String,
}

#[get("/pnl-over-time")]
pub async fn pnl_over_time(
    state: web::Data<AppState<State>>,
    query: web::Query<PnlOverTimeQuery>,
) -> Result<impl Responder, Error> {
    let mut interval = query.interval;

    if interval > 30 {
        interval = 30;
    }

    let data = state
        .database
        .ls_state
        .get_pnl_over_time(query.address.to_owned(), interval)
        .await?;

    Ok(web::Json(data))
}
