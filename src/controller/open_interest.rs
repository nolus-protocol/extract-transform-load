use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "open_interest";

#[get("/open-interest")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.open_interest, CACHE_KEY, || async {
        state.database.ls_state.get_open_interest().await
    })
    .await?;

    Ok(web::Json(ResponseData { open_interest: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub open_interest: BigDecimal,
}
