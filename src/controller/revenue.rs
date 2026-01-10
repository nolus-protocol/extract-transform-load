use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "revenue";

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub revenue: BigDecimal,
}
