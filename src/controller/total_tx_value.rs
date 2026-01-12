use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};

const CACHE_KEY: &str = "total_tx_value";

#[get("/total-tx-value")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.total_tx_value, CACHE_KEY, || async {
        state.database.ls_opening.get_total_tx_value().await
    })
    .await?;

    Ok(web::Json(Response { total_tx_value: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_tx_value: BigDecimal,
}
