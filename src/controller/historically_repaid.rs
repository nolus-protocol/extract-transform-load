use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response, to_streaming_csv_response},
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

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyRepaid {
    pub contract_id: String,
    pub symbol: String,
    pub loan: BigDecimal,
    pub total_repaid: BigDecimal,
    pub close_timestamp: Option<DateTime<Utc>>,
    pub loan_closed: String,
}

impl From<crate::dao::postgre::ls_repayment::HistoricallyRepaid> for HistoricallyRepaid {
    fn from(r: crate::dao::postgre::ls_repayment::HistoricallyRepaid) -> Self {
        Self {
            contract_id: r.contract_id,
            symbol: r.symbol,
            loan: r.loan,
            total_repaid: r.total_repaid,
            close_timestamp: r.close_timestamp,
            loan_closed: r.loan_closed,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HistoricallyRepaidResponse {
    /// Contract ID
    pub contract_id: String,
    /// Asset symbol
    pub symbol: String,
    /// Original loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
    /// Total amount repaid in USD
    #[schema(value_type = f64)]
    pub total_repaid: BigDecimal,
    /// Timestamp when the loan was closed
    pub close_timestamp: Option<DateTime<Utc>>,
    /// Whether the loan was fully closed
    pub loan_closed: String,
}

#[utoipa::path(
    get,
    path = "/api/historically-repaid",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Historically repaid positions with time window filtering", body = Vec<HistoricallyRepaidResponse>)
    )
)]
#[get("/historically-repaid")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("historically_repaid", period_str, query.from);

    if let Some(cached) = state.api_cache.historically_repaid.get(&cache_key).await {
        let data: Vec<HistoricallyRepaid> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "historically-repaid.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .ls_repayment
        .get_historically_repaid_with_window(months, query.from)
        .await?;

    state.api_cache.historically_repaid.set(&cache_key, data.clone()).await;

    let response: Vec<HistoricallyRepaid> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historically-repaid.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[utoipa::path(
    get,
    path = "/api/historically-repaid/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all historically repaid positions. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/historically-repaid/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "historically_repaid_all";

    if let Some(cached) = state.api_cache.historically_repaid.get(CACHE_KEY).await {
        let data: Vec<HistoricallyRepaid> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "historically-repaid.csv");
    }

    let data = state.database.ls_repayment.get_historically_repaid().await?;
    state.api_cache.historically_repaid.set(CACHE_KEY, data.clone()).await;

    let response: Vec<HistoricallyRepaid> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "historically-repaid.csv")
}
