use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{parse_period_months, to_csv_response},
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    protocol: Option<String>,
}

#[get("/utilization-level")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("12m");

    if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        let admin = state.protocols.get(&protocol_key);

        if let Some(protocol) = admin {
            let cache_key = format!("utilization_level_{}_{}", protocol_key, period_str);

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
                .get_utilization_level_with_window(protocol.contracts.lpp.clone(), months)
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
