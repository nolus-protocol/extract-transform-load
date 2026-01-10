use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "open_interest";

#[get("/open-interest")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.open_interest.get(CACHE_KEY).await {
        return Ok(web::Json(ResponseData {
            open_interest: cached,
        }));
    }

    // Cache miss - query DB
    let data = state.database.ls_state.get_open_interest().await?;

    // Store in cache
    state.api_cache.open_interest.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(ResponseData {
        open_interest: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub open_interest: BigDecimal,
}
