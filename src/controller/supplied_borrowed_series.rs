use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
};

#[get("/supplied-borrowed-history")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let protocol_key = query
        .protocol
        .as_ref()
        .map(|p| p.to_uppercase())
        .unwrap_or_else(|| "total".to_string());
    let base_key = format!("supplied_borrowed_{}", protocol_key);
    let cache_key = build_cache_key(&base_key, period_str, query.from);

    // Try cache first (only if no 'from' filter)
    if query.from.is_none() {
        if let Some(cached) = state.api_cache.supplied_borrowed_history.get(&cache_key).await {
            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&cached, "supplied-borrowed-history.csv"),
                _ => Ok(HttpResponse::Ok().json(cached)),
            };
        }
    }

    // Cache miss - query DB
    let data = if let Some(protocol_key) = &query.protocol {
        let protocol_key = protocol_key.to_uppercase();
        if let Some(protocol) = state.protocols.get(&protocol_key) {
            state
                .database
                .lp_pool_state
                .get_supplied_borrowed_series_with_window(
                    protocol.contracts.lpp.to_owned(),
                    months,
                    query.from,
                )
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
            .get_supplied_borrowed_series_total_with_window(protocols, months, query.from)
            .await?
    };

    // Store in cache (only if no 'from' filter)
    if query.from.is_none() {
        state.api_cache.supplied_borrowed_history.set(&cache_key, data.clone()).await;
    }

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "supplied-borrowed-history.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}
