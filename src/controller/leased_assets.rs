use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/leased-assets",
    tag = "Position Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the total value of leased assets grouped by token symbol. Cache: 1 hour.", body = Vec<LeasedAssetResponse>)
    )
)]
#[get("/leased-assets")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let cache_key = query
        .protocol
        .as_ref()
        .map(|p| format!("leased_assets_{}", p.to_uppercase()))
        .unwrap_or_else(|| "leased_assets_total".to_string());

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

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Filter by protocol (e.g., OSMOSIS-OSMOSIS-USDC)
    protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LeasedAssetResponse {
    /// Token symbol
    pub symbol: String,
    /// Total leased value in USD
    #[schema(value_type = f64)]
    pub value: BigDecimal,
}
