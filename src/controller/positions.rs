use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{to_csv_response, to_streaming_csv_response},
};

/// Single cache key for all positions - eliminates cache explosion from pagination
const CACHE_KEY: &str = "positions_all";

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/positions",
    tag = "Position Analytics",
    params(Query),
    responses(
        (status = 200, description = "Detailed information for all open positions including PnL, prices, and liquidation levels", body = Vec<PositionResponse>)
    )
)]
#[get("/positions")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first with single key
    if let Some(cached) = state.api_cache.positions.get(CACHE_KEY).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "positions.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB for all positions
    let data = state.database.ls_state.get_all_positions().await?;

    // Store in cache with single key
    state.api_cache.positions.set(CACHE_KEY, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "positions.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

/// Streaming CSV export endpoint for all positions.
#[utoipa::path(
    get,
    path = "/api/positions/export",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of all open positions. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/positions/export")]
pub async fn export(
    state: web::Data<AppState<State>>,
) -> Result<HttpResponse, Error> {
    // Try cache first with single key
    if let Some(cached) = state.api_cache.positions.get(CACHE_KEY).await {
        return to_streaming_csv_response(cached, "positions.csv");
    }

    // Cache miss - query DB for all positions
    let data = state.database.ls_state.get_all_positions().await?;

    // Store in cache with single key
    state.api_cache.positions.set(CACHE_KEY, data.clone()).await;

    to_streaming_csv_response(data, "positions.csv")
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PositionResponse {
    /// Opening date
    pub date: String,
    /// Position type (Long/Short)
    #[serde(rename = "type")]
    pub position_type: String,
    /// Token symbol
    pub symbol: String,
    /// Contract ID
    pub contract_id: String,
    /// User wallet address
    pub user: String,
    /// Loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
    /// Down payment amount in USD
    #[schema(value_type = f64)]
    pub down_payment: BigDecimal,
    /// Current lease value in USD
    #[schema(value_type = f64)]
    pub lease_value: BigDecimal,
    /// Profit/Loss in USD
    #[schema(value_type = f64)]
    pub pnl: BigDecimal,
    /// Profit/Loss percentage
    #[schema(value_type = f64)]
    pub pnl_percent: BigDecimal,
    /// Current asset price
    #[schema(value_type = f64)]
    pub current_price: BigDecimal,
    /// Liquidation price threshold
    #[schema(value_type = f64)]
    pub liquidation_price: BigDecimal,
}
