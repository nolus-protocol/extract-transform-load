use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
};

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
    /// Time period filter: 3m (default), 6m, 12m, or all
    #[param(inline, value_type = Option<String>)]
    period: Option<String>,
    /// Only return records after this timestamp (exclusive), for incremental syncing
    from: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/api/buyback",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns NLS token buyback transactions with time window filtering. Cache: 1 hour.", body = Vec<BuybackPoint>)
    )
)]
#[get("/buyback")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("buyback", period_str, query.from);

    // Try cache first
    if let Some(cached) = state.api_cache.buyback.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "buyback.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .tr_profit
        .get_buyback_with_window(months, query.from)
        .await?;

    // Store in cache
    state.api_cache.buyback.set(&cache_key, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "buyback.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BuybackPoint {
    /// Timestamp of the buyback
    pub time: DateTime<Utc>,
    /// Amount bought back in NLS
    #[serde(rename = "Bought-back")]
    #[schema(value_type = f64)]
    pub bought_back: BigDecimal,
}
