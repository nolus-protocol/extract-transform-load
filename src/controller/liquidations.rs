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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Liquidation {
    /// Timestamp of the liquidation
    pub timestamp: DateTime<Utc>,
    /// Token ticker
    pub ticker: String,
    /// Contract ID
    pub contract_id: String,
    /// User wallet address
    pub user: Option<String>,
    /// Type of transaction (partial/full)
    pub transaction_type: Option<String>,
    /// Amount liquidated in USD
    #[schema(value_type = f64)]
    pub liquidation_amount: BigDecimal,
    /// Whether the loan was fully closed
    pub closed_loan: bool,
    /// Original down payment in USD
    #[schema(value_type = f64)]
    pub down_payment: BigDecimal,
    /// Original loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
    /// Price at liquidation
    #[schema(value_type = f64)]
    pub liquidation_price: Option<BigDecimal>,
}

impl From<crate::dao::postgre::ls_liquidation::LiquidationData> for Liquidation {
    fn from(l: crate::dao::postgre::ls_liquidation::LiquidationData) -> Self {
        Self {
            timestamp: l.timestamp,
            ticker: l.ticker,
            contract_id: l.contract_id,
            user: l.user,
            transaction_type: l.transaction_type,
            liquidation_amount: l.liquidation_amount,
            closed_loan: l.closed_loan,
            down_payment: l.down_payment,
            loan: l.loan,
            liquidation_price: l.liquidation_price,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/liquidations",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Liquidation events with time window filtering", body = Vec<Liquidation>)
    )
)]
#[get("/liquidations")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("liquidations", period_str, query.from);

    if let Some(cached) = state.api_cache.liquidations.get(&cache_key).await {
        let data: Vec<Liquidation> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "liquidations.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .ls_liquidation
        .get_liquidations_with_window(months, query.from)
        .await?;

    state.api_cache.liquidations.set(&cache_key, data.clone()).await;

    let response: Vec<Liquidation> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "liquidations.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[utoipa::path(
    get,
    path = "/api/liquidations/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all liquidation events. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/liquidations/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "liquidations_all";

    if let Some(cached) = state.api_cache.liquidations.get(CACHE_KEY).await {
        let data: Vec<Liquidation> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "liquidations.csv");
    }

    let data = state.database.ls_liquidation.get_all_liquidations().await?;
    state.api_cache.liquidations.set(CACHE_KEY, data.clone()).await;

    let response: Vec<Liquidation> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "liquidations.csv")
}
