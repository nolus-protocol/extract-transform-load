use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const CACHE_KEY: &str = "realized_pnl_stats";

#[utoipa::path(
    get,
    path = "/api/realized-pnl-stats",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Returns the aggregate realized profit and loss from all closed positions in USD. Cache: 1 hour.", body = Response)
    )
)]
#[get("/realized-pnl-stats")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.realized_pnl_stats.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            realized_pnl: cached,
        }));
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_loan_closing
        .get_realized_pnl_stats()
        .await?
        + BigDecimal::from_str("2958250")?;

    let result = data.with_scale(2);

    // Store in cache
    state.api_cache.realized_pnl_stats.set(CACHE_KEY, result.clone()).await;

    Ok(web::Json(Response {
        realized_pnl: result,
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total realized PnL in USD
    #[schema(value_type = f64)]
    pub realized_pnl: BigDecimal,
}
