use actix_web::{get, web, HttpResponse};
use serde::Serialize;

use crate::{
    configuration::{AppState, State},
    dao::postgre::lp_pool_state::PoolUtilizationLevel,
    error::Error,
};

/// Single cache key for all utilization levels
const CACHE_KEY: &str = "utilization_levels_all";

/// Response wrapper for batch utilization levels
#[derive(Debug, Serialize)]
pub struct UtilizationLevelsResponse {
    pub protocols: Vec<PoolUtilizationLevel>,
}

/// Batch endpoint to get utilization levels for all pools in a single request.
/// This eliminates the N+1 problem where the webapp calls /utilization-level
/// 6-7 times per page load (once for each protocol).
#[get("/utilization-levels")]
pub async fn index(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.utilization_levels.get(CACHE_KEY).await {
        return Ok(HttpResponse::Ok().json(UtilizationLevelsResponse {
            protocols: cached,
        }));
    }

    // Cache miss - query DB for all utilization levels
    let data = state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;

    // Store in cache
    state
        .api_cache
        .utilization_levels
        .set(CACHE_KEY, data.clone())
        .await;

    Ok(HttpResponse::Ok().json(UtilizationLevelsResponse { protocols: data }))
}
