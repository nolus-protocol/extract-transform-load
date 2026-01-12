use actix_web::{get, web, HttpResponse};
use serde::Serialize;

use crate::{
    configuration::{AppState, State},
    dao::postgre::lp_pool_state::PoolUtilizationLevel,
    error::Error,
};

/// Single cache key for all pools data
const CACHE_KEY: &str = "pools_all";

/// Response wrapper for pools data
#[derive(Debug, Serialize)]
pub struct PoolsResponse {
    pub protocols: Vec<PoolUtilizationLevel>,
}

/// Batch endpoint to get pool data for all pools in a single request.
/// Returns utilization levels, supplied/borrowed amounts, and borrow APR.
/// This eliminates the N+1 problem where the webapp calls /utilization-level
/// multiple times per page load (once for each protocol).
#[get("/pools")]
pub async fn index(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.pools.get(CACHE_KEY).await {
        return Ok(HttpResponse::Ok().json(PoolsResponse {
            protocols: cached,
        }));
    }

    // Cache miss - query DB for all pools data
    let data = state
        .database
        .lp_pool_state
        .get_all_utilization_levels()
        .await?;

    // Store in cache
    state
        .api_cache
        .pools
        .set(CACHE_KEY, data.clone())
        .await;

    Ok(HttpResponse::Ok().json(PoolsResponse { protocols: data }))
}
