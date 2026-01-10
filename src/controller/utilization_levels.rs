use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

/// Single cache key for all utilization levels
const CACHE_KEY: &str = "utilization_levels_all";

/// Response wrapper for batch utilization levels
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UtilizationLevelsResponse {
    /// List of utilization levels per protocol
    pub protocols: Vec<ProtocolUtilization>,
}

/// Utilization data for a single protocol
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ProtocolUtilization {
    /// Pool identifier
    pub pool_id: String,
    /// Protocol identifier
    pub protocol: String,
    /// Current utilization percentage
    #[schema(value_type = String)]
    pub utilization: BigDecimal,
    /// Total supplied amount
    #[schema(value_type = String)]
    pub supplied: BigDecimal,
    /// Total borrowed amount
    #[schema(value_type = String)]
    pub borrowed: BigDecimal,
}

/// Batch endpoint to get utilization levels for all pools in a single request.
/// This eliminates the N+1 problem where the webapp calls /utilization-level
/// 6-7 times per page load (once for each protocol).
#[utoipa::path(
    get,
    path = "/api/utilization-levels",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns current utilization levels for all pools in a single request. Eliminates N+1 queries when displaying multiple pools. Cache: 30 min.", body = UtilizationLevelsResponse)
    )
)]
#[get("/utilization-levels")]
pub async fn index(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.utilization_levels.get(CACHE_KEY).await {
        return Ok(HttpResponse::Ok().json(UtilizationLevelsResponse {
            protocols: cached.into_iter().map(|p| ProtocolUtilization {
                pool_id: p.pool_id,
                protocol: p.protocol,
                utilization: p.utilization,
                supplied: p.supplied,
                borrowed: p.borrowed,
            }).collect(),
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

    Ok(HttpResponse::Ok().json(UtilizationLevelsResponse {
        protocols: data.into_iter().map(|p| ProtocolUtilization {
            pool_id: p.pool_id,
            protocol: p.protocol,
            utilization: p.utilization,
            supplied: p.supplied,
            borrowed: p.borrowed,
        }).collect(),
    }))
}
