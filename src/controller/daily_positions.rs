use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
    model::DailyPositionsPoint,
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    /// Only return records after this timestamp (exclusive), for incremental syncing
    from: Option<DateTime<Utc>>,
}

#[get("/daily-positions")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("daily_positions", period_str, query.from);

    if let Some(cached) = state.api_cache.daily_positions.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "daily-positions.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    let data = state
        .database
        .ls_opening
        .get_daily_opened_closed_with_window(months, query.from)
        .await?;
    let series: Vec<DailyPositionsPoint> = data
        .into_iter()
        .map(|(date, closed, opened)| DailyPositionsPoint {
            date,
            closed_loans: closed,
            opened_loans: opened,
        })
        .collect();

    state.api_cache.daily_positions.set(&cache_key, series.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&series, "daily-positions.csv"),
        _ => Ok(HttpResponse::Ok().json(series)),
    }
}
