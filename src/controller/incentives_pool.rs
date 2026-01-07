use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "incentives_pool";

#[get("/incentives-pool")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.incentives_pool.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            incentives_pool: cached,
        }));
    }

    let data = state.database.tr_state.get_incentives_pool().await?;
    state.api_cache.incentives_pool.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        incentives_pool: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub incentives_pool: BigDecimal,
}
