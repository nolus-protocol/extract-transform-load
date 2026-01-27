//! Position-related API endpoints
//!
//! Endpoints for positions, buckets, daily positions, and position analytics.

use actix_web::{get, web, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    cache_keys,
    configuration::{AppState, State},
    error::Error,
    helpers::{
        build_cache_key, cached_fetch, parse_period_months, to_csv_response,
        to_streaming_csv_response,
    },
    model::{DailyPositionsPoint, LS_Amount, PositionBucket, TokenPosition},
};

// =============================================================================
// All Positions
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PositionsQuery {
    format: Option<String>,
    export: Option<bool>,
}

#[get("/positions")]
pub async fn positions(
    state: web::Data<AppState<State>>,
    query: web::Query<PositionsQuery>,
) -> Result<HttpResponse, Error> {
    let data = cached_fetch(
        &state.api_cache.positions,
        cache_keys::POSITIONS,
        || async { Ok(state.database.ls_state.get_all_positions().await?) },
    )
    .await?;

    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        return to_streaming_csv_response(data, "positions.csv");
    }

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "positions.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

// =============================================================================
// Position Buckets
// =============================================================================

#[get("/position-buckets")]
pub async fn position_buckets(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(
        &state.api_cache.position_buckets,
        cache_keys::POSITION_BUCKETS,
        || async {
            let data = state.database.ls_state.get_position_buckets().await?;
            let buckets: Vec<PositionBucket> = data
                .into_iter()
                .map(|b| PositionBucket {
                    loan_category: b.loan_category.unwrap_or_default(),
                    loan_count: b.loan_count,
                    loan_size: b.loan_size,
                })
                .collect();
            Ok(buckets)
        },
    )
    .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Daily Positions
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct DailyPositionsQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/daily-positions")]
pub async fn daily_positions(
    state: web::Data<AppState<State>>,
    query: web::Query<DailyPositionsQuery>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("daily_positions", period_str, query.from);

    let data =
        cached_fetch(&state.api_cache.daily_positions, &cache_key, || async {
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
            Ok(series)
        })
        .await?;

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "daily-positions.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

// =============================================================================
// Open Positions by Token
// =============================================================================

#[get("/open-positions-by-token")]
pub async fn open_positions_by_token(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(
        &state.api_cache.open_positions_by_token,
        cache_keys::OPEN_POSITIONS_BY_TOKEN,
        || async {
            let data = state
                .database
                .ls_state
                .get_open_positions_by_token()
                .await?;
            let token_positions: Vec<TokenPosition> = data
                .into_iter()
                .map(|p| TokenPosition {
                    token: p.token,
                    market_value: p.market_value,
                })
                .collect();
            Ok(token_positions)
        },
    )
    .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Position Debt Value
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PositionDebtValueQuery {
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PositionDebtValueResponse {
    pub position: Vec<LS_Amount>,
    pub debt: Vec<LS_Amount>,
}

#[get("/position-debt-value")]
pub async fn position_debt_value(
    state: web::Data<AppState<State>>,
    query: web::Query<PositionDebtValueQuery>,
) -> Result<impl Responder, Error> {
    let address = query.address.to_lowercase().to_owned();

    let position_fn = state
        .database
        .ls_opening
        .get_position_value(address.to_owned());
    let debt_fn = state.database.ls_opening.get_debt_value(address.to_owned());

    let (position, debt) = tokio::try_join!(position_fn, debt_fn)?;

    Ok(web::Json(PositionDebtValueResponse { position, debt }))
}
