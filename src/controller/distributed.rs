use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "distributed";

#[get("/distributed")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.distributed.get(CACHE_KEY).await {
        return Ok(web::Json(Response { distributed: cached }));
    }

    let data = state
        .database
        .tr_rewards_distribution
        .get_distributed()
        .await?;
    state.api_cache.distributed.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response { distributed: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub distributed: BigDecimal,
}
