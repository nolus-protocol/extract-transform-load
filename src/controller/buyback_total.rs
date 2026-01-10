use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "buyback_total";

#[utoipa::path(
    get,
    path = "/api/buyback-total",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the total amount spent on NLS token buybacks in USD. Cache: 1 hour.", body = Response)
    )
)]
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total buyback amount in USD
    #[schema(value_type = f64)]
    pub buyback_total: BigDecimal,
}
