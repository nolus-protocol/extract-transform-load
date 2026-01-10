use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "buyback_total";

#[get("/buyback-total")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.buyback_total.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            buyback_total: cached,
        }));
    }

    let data = state.database.tr_profit.get_buyback_total().await?;
    state.api_cache.buyback_total.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        buyback_total: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub buyback_total: BigDecimal,
}
