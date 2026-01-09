use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response, to_streaming_csv_response},
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    /// Only return records after this timestamp (exclusive), for incremental syncing
    from: Option<DateTime<Utc>>,
}

#[get("/realized-pnl-wallet")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("12m");
    let cache_key = build_cache_key("realized_pnl_wallet", period_str, query.from);

    // Try cache first
    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "realized-pnl-wallet.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(months, query.from)
        .await?;

    // Store in cache
    state.api_cache.realized_pnl_wallet.set(&cache_key, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "realized-pnl-wallet.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

#[get("/realized-pnl-wallet/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
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
