use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
    model::RevenueSeriesPoint,
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

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
