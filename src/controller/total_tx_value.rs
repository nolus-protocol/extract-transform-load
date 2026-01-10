use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "total_tx_value";

#[get("/total-tx-value")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.total_tx_value.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            total_tx_value: cached,
        }));
    }

    // Cache miss - query DB
    let data = state.database.ls_opening.get_total_tx_value().await?;

    // Store in cache
    state.api_cache.total_tx_value.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        total_tx_value: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_tx_value: BigDecimal,
}
