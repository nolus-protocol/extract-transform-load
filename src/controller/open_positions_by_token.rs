use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TokenPosition,
};

const CACHE_KEY: &str = "open_positions_by_token";

#[utoipa::path(
    get,
    path = "/api/open-positions-by-token",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Returns open positions grouped by token with their current market values. Cache: 1 hour.", body = Vec<TokenPositionResponse>)
    )
)]
#[get("/open-positions-by-token")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.open_positions_by_token.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state.database.ls_state.get_open_positions_by_token().await?;
    let positions: Vec<TokenPosition> = data
        .into_iter()
        .map(|p| TokenPosition {
            token: p.token,
            market_value: p.market_value,
        })
        .collect();

    state.api_cache.open_positions_by_token.set(CACHE_KEY, positions.clone()).await;

    Ok(web::Json(positions))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TokenPositionResponse {
    /// Token symbol
    pub token: String,
    /// Market value in USD
    #[schema(value_type = f64)]
    pub market_value: BigDecimal,
}
