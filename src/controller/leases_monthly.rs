use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const CACHE_KEY: &str = "leases_monthly";

#[utoipa::path(
    get,
    path = "/api/leases-monthly",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Returns monthly aggregated lease count and total volume statistics. Cache: 1 hour.", body = Vec<LeasesMonthlyResponse>)
    )
)]
#[get("/leases-monthly")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.leases_monthly.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    // Cache miss - query DB
    let data = state.database.ls_opening.get_leases_monthly().await?;

    // Store in cache
    state.api_cache.leases_monthly.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(data))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LeasesMonthlyResponse {
    /// Month in YYYY-MM format
    pub month: String,
    /// Number of leases opened
    pub lease_count: i64,
    /// Total lease value in USD
    #[schema(value_type = f64)]
    pub total_value: BigDecimal,
}
