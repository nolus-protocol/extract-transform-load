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
pub struct HistoricallyOpened {
    pub contract_id: String,
    pub user: String,
    pub leased_asset: String,
    pub opening_date: DateTime<Utc>,
    pub position_type: String,
    pub down_payment_amount: BigDecimal,
    pub down_payment_asset: String,
    pub loan: BigDecimal,
    pub total_position_amount_lpn: BigDecimal,
    pub price: Option<BigDecimal>,
    pub open: bool,
    pub liquidation_price: Option<BigDecimal>,
}

impl From<crate::dao::postgre::ls_opening::HistoricallyOpened> for HistoricallyOpened {
    fn from(o: crate::dao::postgre::ls_opening::HistoricallyOpened) -> Self {
        Self {
            contract_id: o.contract_id,
            user: o.user,
            leased_asset: o.leased_asset,
            opening_date: o.opening_date,
            position_type: o.position_type,
            down_payment_amount: o.down_payment_amount,
            down_payment_asset: o.down_payment_asset,
            loan: o.loan,
            total_position_amount_lpn: o.total_position_amount_lpn,
            price: o.price,
            open: o.open,
            liquidation_price: o.liquidation_price,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct HistoricallyOpenedResponse {
    /// Contract ID
    pub contract_id: String,
    /// User wallet address
    pub user: String,
    /// Leased asset symbol
    pub leased_asset: String,
    /// Opening date
    pub opening_date: DateTime<Utc>,
    /// Position type (Long/Short)
    pub position_type: String,
    /// Down payment amount
    #[schema(value_type = f64)]
    pub down_payment_amount: BigDecimal,
    /// Down payment asset symbol
    pub down_payment_asset: String,
    /// Loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
    /// Total position amount in LPN
    #[schema(value_type = f64)]
    pub total_position_amount_lpn: BigDecimal,
    /// Opening price
    #[schema(value_type = f64)]
    pub price: Option<BigDecimal>,
    /// Whether the position is still open
    pub open: bool,
    /// Liquidation price threshold
    #[schema(value_type = f64)]
    pub liquidation_price: Option<BigDecimal>,
}

#[utoipa::path(
    get,
    path = "/api/historically-opened",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Historically opened positions with position type, opening price, and liquidation price", body = Vec<HistoricallyOpenedResponse>)
    )
)]
#[get("/historically-opened")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("historically_opened", period_str, query.from);

    if let Some(cached) = state.api_cache.historically_opened.get(&cache_key).await {
        let data: Vec<HistoricallyOpened> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "historically-opened.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .ls_opening
        .get_historically_opened_with_window(months, query.from)
        .await?;

    state.api_cache.historically_opened.set(&cache_key, data.clone()).await;

    let response: Vec<HistoricallyOpened> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historically-opened.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[utoipa::path(
    get,
    path = "/api/historically-opened/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all historically opened positions. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/historically-opened/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "historically_opened_all";

    if let Some(cached) = state.api_cache.historically_opened.get(CACHE_KEY).await {
        let data: Vec<HistoricallyOpened> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "historically-opened.csv");
    }

    let data = state.database.ls_opening.get_all_historically_opened().await?;
    state.api_cache.historically_opened.set(CACHE_KEY, data.clone()).await;

    let response: Vec<HistoricallyOpened> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "historically-opened.csv")
}
