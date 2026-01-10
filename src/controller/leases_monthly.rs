use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};

const CACHE_KEY: &str = "leases_monthly";

#[get("/leases-monthly")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.leases_monthly.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    // Cache miss - query DB
    let data = state.database.ls_opening.get_leases_monthly().await?;

    // Store in cache
    state.api_cache.leases_monthly.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(data))
}
