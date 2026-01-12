use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "supplied_funds";

#[get("/supplied-funds")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.supplied_funds, CACHE_KEY, || async {
        let data = state.database.lp_pool_state.get_supplied_funds().await?;
        Ok(data.with_scale(2))
    })
    .await?;

    Ok(web::Json(ResponseData { amount: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub amount: BigDecimal,
}
