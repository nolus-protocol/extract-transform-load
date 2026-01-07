use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};
use actix_web::{get, web, HttpResponse, Result};
use serde::Deserialize;

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    format: Option<String>,
}

#[get("/realized-pnl-wallet")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let cache_key = format!("realized_pnl_wallet_{}_{}", skip, limit);

    // Try cache first
    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "realized-pnl-wallet.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB
    let items = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet(skip, limit)
        .await?;

    // Store in cache
    state.api_cache.realized_pnl_wallet.set(&cache_key, items.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&items, "realized-pnl-wallet.csv"),
        _ => Ok(HttpResponse::Ok().json(items)),
    }
}
