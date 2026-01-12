use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};

const CACHE_KEY: &str = "distributed";

#[get("/distributed")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.distributed, CACHE_KEY, || async {
        state.database.tr_rewards_distribution.get_distributed().await
    })
    .await?;

    Ok(web::Json(Response { distributed: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub distributed: BigDecimal,
}
