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
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}

impl From<crate::dao::postgre::lp_deposit::HistoricalLender> for HistoricalLender {
    fn from(l: crate::dao::postgre::lp_deposit::HistoricalLender) -> Self {
        Self {
            transaction_type: l.transaction_type,
            timestamp: l.timestamp,
            user: l.user,
            amount: l.amount,
            pool: l.pool,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HistoricalLenderResponse {
    /// Transaction type (Deposit/Withdrawal)
    pub transaction_type: String,
    /// Timestamp of the transaction
    pub timestamp: DateTime<Utc>,
    /// User wallet address
    pub user: String,
    /// Transaction amount in USD
    #[schema(value_type = f64)]
    pub amount: BigDecimal,
    /// Pool name
    pub pool: String,
}

#[utoipa::path(
    get,
    path = "/api/historical-lenders",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Lender deposit and withdrawal history with time window filtering", body = Vec<HistoricalLenderResponse>)
    )
)]
#[get("/historical-lenders")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("historical_lenders", period_str, query.from);

    if let Some(cached) = state.api_cache.historical_lenders.get(&cache_key).await {
        let data: Vec<HistoricalLender> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "historical-lenders.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .lp_deposit
        .get_historical_lenders_with_window(months, query.from)
        .await?;

    state.api_cache.historical_lenders.set(&cache_key, data.clone()).await;

    let response: Vec<HistoricalLender> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historical-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[utoipa::path(
    get,
    path = "/api/historical-lenders/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all lender deposit/withdrawal history. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/historical-lenders/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "historical_lenders_all";

    if let Some(cached) = state.api_cache.historical_lenders.get(CACHE_KEY).await {
        let data: Vec<HistoricalLender> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "historical-lenders.csv");
    }

    let data = state.database.lp_deposit.get_all_historical_lenders().await?;
    state.api_cache.historical_lenders.set(CACHE_KEY, data.clone()).await;

    let response: Vec<HistoricalLender> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "historical-lenders.csv")
}
