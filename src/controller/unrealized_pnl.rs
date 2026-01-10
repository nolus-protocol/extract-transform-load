use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const CACHE_KEY: &str = "unrealized_pnl";

#[utoipa::path(
    get,
    path = "/api/unrealized-pnl",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Returns the aggregate unrealized profit and loss for all open positions in USD. Cache: 1 hour.", body = Response)
    )
)]
#[get("/unrealized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.unrealized_pnl.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            unrealized_pnl: cached,
        }));
    }

    let data = state.database.ls_state.get_unrealized_pnl().await?;
    state.api_cache.unrealized_pnl.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        unrealized_pnl: data,
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total unrealized PnL in USD
    #[schema(value_type = f64)]
    pub unrealized_pnl: BigDecimal,
}
