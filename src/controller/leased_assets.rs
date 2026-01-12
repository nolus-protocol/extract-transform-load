use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::build_protocol_cache_key,
};

#[get("/leased-assets")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let cache_key = build_protocol_cache_key("leased_assets", query.protocol.as_deref());

    // Try cache first
    if let Some(cached) = state.api_cache.leased_assets.get(&cache_key).await {
        return Ok(web::Json(cached));
    }

    // Cache miss - query DB
    let data = if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        if let Some(protocol) = state.protocols.get(&protocol_key) {
            state
                .database
                .ls_opening
                .get_leased_assets(protocol.contracts.lpp.to_owned())
                .await?
        } else {
            vec![]
        }
    } else {
        state.database.ls_opening.get_leased_assets_total().await?
    };

    // Store in cache
    state.api_cache.leased_assets.set(&cache_key, data.clone()).await;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
