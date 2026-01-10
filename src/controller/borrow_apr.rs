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
    path = "/api/borrow-apr",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns historical borrow APR values for a specific protocol. Returns array of APR values (BigDecimal). CSV format includes full data with timestamps. Cache: 1 hour.", body = Vec<String>)
    )
)]
#[get("/borrow-apr")]
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
            let cache_key = build_cache_key_with_protocol("borrow_apr", &protocol_key, period_str, query.from);

            // Try cache first
            if let Some(cached) = state.api_cache.borrow_apr.get(&cache_key).await {
                let items: Vec<BigDecimal> =
                    cached.iter().map(|item| item.APR.clone()).collect();
                return match query.format.as_deref() {
                    Some("csv") => to_csv_response(&cached, "borrow-apr.csv"),
                    _ => Ok(HttpResponse::Ok().json(items)),
                };
            }

            // Cache miss - query DB
            let data = state
                .database
                .ls_opening
                .get_borrow_apr_with_window(protocol.contracts.lpp.clone(), months, query.from)
                .await?;

            // Store in cache
            state.api_cache.borrow_apr.set(&cache_key, data.clone()).await;

            let items: Vec<BigDecimal> =
                data.iter().map(|item| item.APR.clone()).collect();

            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&data, "borrow-apr.csv"),
                _ => Ok(HttpResponse::Ok().json(items)),
            };
        }
    }

    let items: Vec<BigDecimal> = vec![];
    Ok(HttpResponse::Ok().json(items))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BorrowAprPoint {
    /// Timestamp of the APR reading
    pub timestamp: DateTime<Utc>,
    /// Annual Percentage Rate
    #[serde(rename = "APR")]
    #[schema(value_type = String)]
    pub apr: BigDecimal,
}
