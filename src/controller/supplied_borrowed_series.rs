use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/supplied-borrowed-history")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let cache_key = query
        .protocol
        .as_ref()
        .map(|p| format!("supplied_borrowed_{}", p.to_uppercase()))
        .unwrap_or_else(|| "supplied_borrowed_total".to_string());

    // Try cache first
    if let Some(cached) = state.api_cache.supplied_borrowed_history.get(&cache_key).await {
        return Ok(web::Json(cached));
    }

    // Cache miss - query DB
    let data = if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        if let Some(protocol) = state.protocols.get(&protocol_key) {
            state
                .database
                .lp_pool_state
                .get_supplied_borrowed_series(protocol.contracts.lpp.to_owned())
                .await?
        } else {
            vec![]
        }
    } else {
        let protocols: Vec<String> = state
            .protocols
            .values()
            .map(|p| p.contracts.lpp.to_owned())
            .collect();

        state
            .database
            .lp_pool_state
            .get_supplied_borrowed_series_total(protocols)
            .await?
    };

    // Store in cache
    state.api_cache.supplied_borrowed_history.set(&cache_key, data.clone()).await;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
