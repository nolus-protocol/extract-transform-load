use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TvlPoolParams,
};

const CACHE_KEY: &str = "tvl";

#[get("/total-value-locked")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.total_value_locked.get(CACHE_KEY).await {
        return Ok(web::Json(Response {
            total_value_locked: cached,
        }));
    }

    // Cache miss - query DB
    let pools = &state.config.lp_pools;

    let get_pool = |idx: usize, name: &str| -> Result<String, Error> {
        pools
            .get(idx)
            .map(|(id, _, _, _)| id.clone())
            .ok_or_else(|| Error::ProtocolError(name.to_string()))
    };

    let data = state
        .database
        .ls_state
        .get_total_value_locked(TvlPoolParams {
            osmosis_usdc: get_pool(1, "osmosis_usdc")?,
            neutron_axelar: get_pool(2, "neutron_usdc_axelar")?,
            osmosis_usdc_noble: get_pool(3, "osmosis_usdc_noble")?,
            neutron_usdc_noble: get_pool(0, "neutron_usdc_noble")?,
            osmosis_st_atom: get_pool(4, "osmosis_st_atom")?,
            osmosis_all_btc: get_pool(5, "osmosis_all_btc")?,
            osmosis_all_sol: get_pool(6, "osmosis_all_sol")?,
            osmosis_akt: get_pool(7, "osmosis_akt")?,
        })
        .await?;

    // Store in cache
    state.api_cache.total_value_locked.set(CACHE_KEY, data.clone()).await;

    Ok(web::Json(Response {
        total_value_locked: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_value_locked: BigDecimal,
}
