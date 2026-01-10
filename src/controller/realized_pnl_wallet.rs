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
pub struct RealizedPnlWalletResponse {
    /// Wallet address
    pub wallet: String,
    /// Total realized PnL in USD
    #[schema(value_type = f64)]
    pub realized_pnl: BigDecimal,
    /// Number of trades
    pub trade_count: i64,
}

#[utoipa::path(
    get,
    path = "/api/realized-pnl-wallet",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Realized PnL aggregated per wallet address with trade counts", body = Vec<RealizedPnlWalletResponse>)
    )
)]
#[get("/realized-pnl-wallet")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("realized_pnl_wallet", period_str, query.from);

    // Try cache first
    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(&cache_key).await {
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&cached, "realized-pnl-wallet.csv"),
            _ => Ok(HttpResponse::Ok().json(cached)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(months, query.from)
        .await?;

    // Store in cache
    state.api_cache.realized_pnl_wallet.set(&cache_key, data.clone()).await;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "realized-pnl-wallet.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

#[utoipa::path(
    get,
    path = "/api/realized-pnl-wallet/export",
    tag = "Lending Analytics",
    responses(
        (status = 200, description = "Streaming CSV export of realized PnL by wallet. Cache: 1 hour.", content_type = "text/csv")
    )
)]
#[get("/realized-pnl-wallet/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "realized_pnl_wallet_all";

    if let Some(cached) = state.api_cache.realized_pnl_wallet.get(CACHE_KEY).await {
        return to_streaming_csv_response(cached, "realized-pnl-wallet.csv");
    }

    let data = state
        .database
        .ls_opening
        .get_realized_pnl_by_wallet_with_window(None, None)
        .await?;

    state.api_cache.realized_pnl_wallet.set(CACHE_KEY, data.clone()).await;

    to_streaming_csv_response(data, "realized-pnl-wallet.csv")
}
