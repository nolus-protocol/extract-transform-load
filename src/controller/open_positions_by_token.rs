use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TokenPosition,
};

const CACHE_KEY: &str = "open_positions_by_token";

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
