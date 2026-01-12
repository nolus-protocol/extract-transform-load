use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "unrealized_pnl";

#[get("/unrealized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.unrealized_pnl, CACHE_KEY, || async {
        state.database.ls_state.get_unrealized_pnl().await
    })
    .await?;

    Ok(web::Json(ResponseData { unrealized_pnl: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub unrealized_pnl: BigDecimal,
}
