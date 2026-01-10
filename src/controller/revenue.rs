use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "revenue";

#[utoipa::path(
    get,
    path = "/api/revenue",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the total protocol revenue generated from fees and interest in USD. Cache: 30 min.", body = Response)
    )
)]
#[get("/revenue")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.revenue.get(CACHE_KEY).await {
        return Ok(web::Json(Response { revenue: cached }));
    }

    // Cache miss - query DB
    let data = state.database.tr_profit.get_revenue().await?;

    // Store in cache
    state.api_cache.revenue.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response { revenue: data }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total revenue in USD
    #[schema(value_type = f64)]
    pub revenue: BigDecimal,
}
