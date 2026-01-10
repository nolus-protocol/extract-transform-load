use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
    model::RevenueSeriesPoint,
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
    path = "/api/revenue-series",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns daily and cumulative revenue over time for trend analysis. Cache: 1 hour.", body = Vec<RevenueSeriesPointResponse>)
    )
)]
#[get("/revenue-series")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("revenue_series", period_str, query.from);

    // Try cache first (only if no 'from' filter)
    if query.from.is_none() {
        if let Some(cached) = state.api_cache.revenue_series.get(&cache_key).await {
            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&cached, "revenue-series.csv"),
                _ => Ok(HttpResponse::Ok().json(cached)),
            };
        }
    }

    let data = state
        .database
        .tr_profit
        .get_revenue_series_with_window(months, query.from)
        .await?;
    let series: Vec<RevenueSeriesPoint> = data
        .into_iter()
        .map(|(time, daily, cumulative)| RevenueSeriesPoint {
            time,
            daily,
            cumulative,
        })
        .collect();

    // Store in cache (only if no 'from' filter)
    if query.from.is_none() {
        state.api_cache.revenue_series.set(&cache_key, series.clone()).await;
    }

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&series, "revenue-series.csv"),
        _ => Ok(HttpResponse::Ok().json(series)),
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RevenueSeriesPointResponse {
    /// Timestamp of the data point
    pub time: DateTime<Utc>,
    /// Daily revenue in USD
    #[schema(value_type = f64)]
    pub daily: BigDecimal,
    /// Cumulative revenue in USD
    #[schema(value_type = f64)]
    pub cumulative: BigDecimal,
}
