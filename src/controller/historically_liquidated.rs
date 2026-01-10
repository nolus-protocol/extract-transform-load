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
pub struct HistoricallyLiquidated {
    pub contract_id: String,
    pub asset: String,
    pub loan: BigDecimal,
    pub total_liquidated: Option<BigDecimal>,
}

impl From<crate::dao::postgre::ls_liquidation::HistoricallyLiquidated> for HistoricallyLiquidated {
    fn from(l: crate::dao::postgre::ls_liquidation::HistoricallyLiquidated) -> Self {
        Self {
            contract_id: l.contract_id,
            asset: l.asset,
            loan: l.loan,
            total_liquidated: l.total_liquidated,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HistoricallyLiquidatedResponse {
    /// Contract ID
    pub contract_id: String,
    /// Asset symbol
    pub asset: String,
    /// Original loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
    /// Total amount liquidated in USD
    #[schema(value_type = f64)]
    pub total_liquidated: Option<BigDecimal>,
}

#[utoipa::path(
    get,
    path = "/api/historically-liquidated",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Historically liquidated positions with time window filtering", body = Vec<HistoricallyLiquidatedResponse>)
    )
)]
#[get("/historically-liquidated")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("historically_liquidated", period_str, query.from);

    if let Some(cached) = state.api_cache.historically_liquidated.get(&cache_key).await {
        let data: Vec<HistoricallyLiquidated> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "historically-liquidated.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .ls_liquidation
        .get_historically_liquidated_with_window(months, query.from)
        .await?;

    state.api_cache.historically_liquidated.set(&cache_key, data.clone()).await;

    let response: Vec<HistoricallyLiquidated> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historically-liquidated.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[utoipa::path(
    get,
    path = "/api/historically-liquidated/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all historically liquidated positions. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/historically-liquidated/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "historically_liquidated_all";

    if let Some(cached) = state.api_cache.historically_liquidated.get(CACHE_KEY).await {
        let data: Vec<HistoricallyLiquidated> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "historically-liquidated.csv");
    }

    let data = state.database.ls_liquidation.get_historically_liquidated().await?;
    state.api_cache.historically_liquidated.set(CACHE_KEY, data.clone()).await;

    let response: Vec<HistoricallyLiquidated> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "historically-liquidated.csv")
}
