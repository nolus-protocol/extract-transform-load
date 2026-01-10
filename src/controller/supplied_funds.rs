use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const CACHE_KEY: &str = "supplied_funds";

#[utoipa::path(
    get,
    path = "/api/supplied-funds",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the total funds supplied by lenders across all pools in USD. Cache: 30 min.", body = Response)
    )
)]
#[get("/supplied-funds")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.supplied_funds.get(CACHE_KEY).await {
        return Ok(web::Json(Response { supplied_funds: cached }));
    }

    // Cache miss - query DB
    let data = state.database.lp_pool_state.get_supplied_funds().await?;
    let result = data.with_scale(2);

    // Store in cache
    state.api_cache.supplied_funds.set(CACHE_KEY, result.clone()).await;

    Ok(web::Json(Response { supplied_funds: result }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total supplied funds in USD
    #[schema(value_type = f64)]
    pub supplied_funds: BigDecimal,
}
