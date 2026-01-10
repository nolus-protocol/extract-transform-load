use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key_with_protocol, parse_period_months, to_csv_response},
};

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format (csv includes timestamps)
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
    /// Time period filter: 3m (default), 6m, 12m, or all
    #[param(inline, value_type = Option<String>)]
    period: Option<String>,
    /// Protocol identifier (e.g., OSMOSIS-OSMOSIS-USDC) - required
    protocol: Option<String>,
    /// Only return records after this timestamp (exclusive), for incremental syncing
    from: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/api/utilization-level",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns historical pool utilization values for a specific protocol. Returns array of utilization percentages (BigDecimal). CSV format includes full data with timestamps. Cache: 30 min.", body = Vec<String>)
    )
)]
#[get("/utilization-level")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");

    if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        let admin = state.protocols.get(&protocol_key);

        if let Some(protocol) = admin {
            let cache_key = build_cache_key_with_protocol("utilization_level", &protocol_key, period_str, query.from);

            // Try cache first
            if let Some(cached) = state.api_cache.utilization_level.get(&cache_key).await {
                let items: Vec<BigDecimal> = cached
                    .iter()
                    .map(|item| item.utilization_level.clone())
                    .collect();
                return match query.format.as_deref() {
                    Some("csv") => to_csv_response(&cached, "utilization-level.csv"),
                    _ => Ok(HttpResponse::Ok().json(items)),
                };
            }

            // Cache miss - query DB
            let data = state
                .database
                .lp_pool_state
                .get_utilization_level_with_window(protocol.contracts.lpp.clone(), months, query.from)
                .await?;

            // Store in cache
            state.api_cache.utilization_level.set(&cache_key, data.clone()).await;

            let items: Vec<BigDecimal> = data
                .iter()
                .map(|item| item.utilization_level.clone())
                .collect();

            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&data, "utilization-level.csv"),
                _ => Ok(HttpResponse::Ok().json(items)),
            };
        }
    }

    // No protocol specified - return empty array (legacy behavior)
    let items: Vec<BigDecimal> = vec![];
    Ok(HttpResponse::Ok().json(items))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UtilizationPoint {
    /// Timestamp of the utilization reading
    pub timestamp: DateTime<Utc>,
    /// Utilization level percentage
    #[schema(value_type = String)]
    pub utilization_level: BigDecimal,
}
