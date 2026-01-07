use actix_web::{get, web, HttpResponse};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    format: Option<String>,
}

#[get("/positions")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let cache_key = format!("positions_{}_{}", skip, limit);

    // Try cache first
    if let Some(cached) = state.api_cache.positions.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "positions.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB
    let data = state.database.ls_state.get_positions(skip, limit).await?;

    // Store in cache
    state.api_cache.positions.set(&cache_key, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "positions.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}
