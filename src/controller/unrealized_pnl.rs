use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "unrealized_pnl";

#[get("/unrealized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.unrealized_pnl.get(CACHE_KEY).await {
        return Ok(web::Json(ResponseData {
            unrealized_pnl: cached,
        }));
    }

    let data = state.database.ls_state.get_unrealized_pnl().await?;
    state.api_cache.unrealized_pnl.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(ResponseData {
        unrealized_pnl: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub unrealized_pnl: BigDecimal,
}
