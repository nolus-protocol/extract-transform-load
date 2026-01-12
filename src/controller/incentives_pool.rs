use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};

const CACHE_KEY: &str = "incentives_pool";

#[get("/incentives-pool")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.incentives_pool, CACHE_KEY, || async {
        state.database.tr_state.get_incentives_pool().await
    })
    .await?;

    Ok(web::Json(Response { incentives_pool: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub incentives_pool: BigDecimal,
}
