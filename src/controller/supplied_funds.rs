use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "supplied_funds";

#[get("/supplied-funds")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.supplied_funds.get(CACHE_KEY).await {
        return Ok(web::Json(ResponseData { amount: cached }));
    }

    // Cache miss - query DB
    let data = state.database.lp_pool_state.get_supplied_funds().await?;
    let result = data.with_scale(2);

    // Store in cache
    state.api_cache.supplied_funds.set(CACHE_KEY, result.clone()).await;

    Ok(web::Json(ResponseData { amount: result }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub amount: BigDecimal,
}
