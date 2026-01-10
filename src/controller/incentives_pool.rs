use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "incentives_pool";

#[utoipa::path(
    get,
    path = "/api/incentives-pool",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the current balance of the incentives pool in USD. Cache: 1 hour.", body = Response)
    )
)]
#[get("/incentives-pool")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.incentives_pool.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            incentives_pool: cached,
        }));
    }

    let data = state.database.tr_state.get_incentives_pool().await?;
    state.api_cache.incentives_pool.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        incentives_pool: data,
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Incentives pool balance in USD
    #[schema(value_type = f64)]
    pub incentives_pool: BigDecimal,
}
