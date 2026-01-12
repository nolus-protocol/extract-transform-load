use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::build_protocol_cache_key,
};

#[get("/borrowed")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let cache_key = build_protocol_cache_key("borrowed", data.protocol.as_deref());

    // Try cache first
    if let Some(cached) = state.api_cache.borrowed.get(&cache_key).await {
        return Ok(web::Json(Response { borrowed: cached }));
    }

    // Cache miss - query DB
    let borrowed = if let Some(protocol_key) = &data.protocol {
        let protocol_key = protocol_key.to_uppercase();
        if let Some(protocol) = state.protocols.get(&protocol_key) {
            state
                .database
                .ls_opening
                .get_borrowed(protocol.contracts.lpp.to_owned())
                .await?
        } else {
            BigDecimal::from(0)
        }
    } else {
        state.database.ls_opening.get_borrowed_total().await?
    };

    // Store in cache
    state.api_cache.borrowed.set(&cache_key, borrowed.clone()).await;

    Ok(web::Json(Response { borrowed }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub borrowed: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
