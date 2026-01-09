use actix_web::{get, web, HttpResponse};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{to_csv_response, to_streaming_csv_response},
};

/// Single cache key for all positions - eliminates cache explosion from pagination
const CACHE_KEY: &str = "positions_all";

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
}

#[get("/positions")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first with single key
    if let Some(cached) = state.api_cache.positions.get(CACHE_KEY).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "positions.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB for all positions
    let data = state.database.ls_state.get_all_positions().await?;

    // Store in cache with single key
    state.api_cache.positions.set(CACHE_KEY, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "positions.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

/// Streaming CSV export endpoint for all positions.
#[get("/positions/export")]
pub async fn export(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    // Try cache first with single key
    if let Some(cached) = state.api_cache.positions.get(CACHE_KEY).await {
        return to_streaming_csv_response(cached, "positions.csv");
    }

    // Cache miss - query DB for all positions
    let data = state.database.ls_state.get_all_positions().await?;

    // Store in cache with single key
    state.api_cache.positions.set(CACHE_KEY, data.clone()).await;

    to_streaming_csv_response(data, "positions.csv")
}
