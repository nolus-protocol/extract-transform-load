use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const CACHE_KEY: &str = "distributed";

#[utoipa::path(
    get,
    path = "/api/distributed",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the total rewards distributed to stakers in USD. Cache: 1 hour.", body = Response)
    )
)]
#[get("/distributed")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.distributed.get(CACHE_KEY).await {
        return Ok(web::Json(Response { distributed: cached }));
    }

    let data = state
        .database
        .tr_rewards_distribution
        .get_distributed()
        .await?;
    state.api_cache.distributed.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response { distributed: data }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total distributed rewards in USD
    #[schema(value_type = f64)]
    pub distributed: BigDecimal,
}
