//! Platform metrics API endpoints
//!
//! Endpoints for TVL, borrowed amounts, supplied funds, open interest, and time series data.

use actix_web::{get, web, HttpResponse, Responder};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use etl_core::{
    cache_keys,
    configuration::{AppState, State},
    helpers::{
        build_cache_key, build_protocol_cache_key, cached_fetch,
        parse_period_months,
    },
    model::MonthlyActiveWallet,
};

use crate::csv_response::to_csv_response;

// =============================================================================
// Total Value Locked
// =============================================================================

#[get("/total-value-locked")]
pub async fn total_value_locked(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.total_value_locked,
        cache_keys::TVL,
        || async {
            let pool_ids = state.get_active_pool_ids();
            state
                .database
                .ls_state
                .get_total_value_locked(pool_ids)
                .await
        },
    )
    .await?;

    Ok(web::Json(TvlResponse {
        total_value_locked: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TvlResponse {
    pub total_value_locked: BigDecimal,
}

// =============================================================================
// Total Transaction Value
// =============================================================================

#[get("/total-tx-value")]
pub async fn total_tx_value(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.total_tx_value,
        cache_keys::TOTAL_TX_VALUE,
        || async { state.database.ls_opening.get_total_tx_value().await },
    )
    .await?;

    Ok(web::Json(TotalTxValueResponse {
        total_tx_value: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TotalTxValueResponse {
    pub total_tx_value: BigDecimal,
}

// =============================================================================
// Supplied Funds
// =============================================================================

#[get("/supplied-funds")]
pub async fn supplied_funds(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.supplied_funds,
        cache_keys::SUPPLIED_FUNDS,
        || async {
            let data =
                state.database.lp_pool_state.get_supplied_funds().await?;
            Ok(data.with_scale(2))
        },
    )
    .await?;

    Ok(web::Json(SuppliedFundsResponse { amount: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SuppliedFundsResponse {
    pub amount: BigDecimal,
}

// =============================================================================
// Open Interest
// =============================================================================

#[get("/open-interest")]
pub async fn open_interest(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.open_interest,
        cache_keys::OPEN_INTEREST,
        || async { state.database.ls_state.get_open_interest().await },
    )
    .await?;

    Ok(web::Json(OpenInterestResponse {
        open_interest: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenInterestResponse {
    pub open_interest: BigDecimal,
}

// =============================================================================
// Open Position Value
// =============================================================================

#[get("/open-position-value")]
pub async fn open_position_value(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.open_position_value,
        cache_keys::OPEN_POSITION_VALUE,
        || async { state.database.ls_state.get_open_position_value().await },
    )
    .await?;

    Ok(web::Json(OpenPositionValueResponse {
        open_position_value: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenPositionValueResponse {
    pub open_position_value: BigDecimal,
}

// =============================================================================
// Borrowed
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct BorrowedQuery {
    protocol: Option<String>,
}

#[get("/borrowed")]
pub async fn borrowed(
    state: web::Data<AppState<State>>,
    query: web::Query<BorrowedQuery>,
) -> Result<impl Responder, crate::error::ApiError> {
    let cache_key =
        build_protocol_cache_key("borrowed", query.protocol.as_deref());
    let protocol = query.protocol.clone();

    let borrowed =
        cached_fetch(&state.api_cache.borrowed, &cache_key, || async {
            let result = if let Some(protocol_key) = &protocol {
                let protocol_key = protocol_key.to_uppercase();
                if let Some(protocol) = state.protocols.get(&protocol_key) {
                    state
                        .database
                        .ls_opening
                        .get_borrowed(protocol.contracts.lpp.to_owned())
                        .await?
                } else {
                    BigDecimal::from(0)
                }
            } else {
                state.database.ls_opening.get_borrowed_total().await?
            };
            Ok(result)
        })
        .await?;

    Ok(web::Json(BorrowedResponse { borrowed }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BorrowedResponse {
    pub borrowed: BigDecimal,
}

// =============================================================================
// Supplied/Borrowed History
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SuppliedBorrowedQuery {
    protocol: Option<String>,
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/supplied-borrowed-history")]
pub async fn supplied_borrowed_history(
    state: web::Data<AppState<State>>,
    query: web::Query<SuppliedBorrowedQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let protocol_key = query
        .protocol
        .as_ref()
        .map(|p| p.to_uppercase())
        .unwrap_or_else(|| "total".to_string());
    let base_key = format!("supplied_borrowed_{}", protocol_key);
    let cache_key = build_cache_key(&base_key, period_str, query.from);
    let protocol = query.protocol.clone();

    let fetch = || async {
        let data = if let Some(protocol_key) = &protocol {
            let protocol_key = protocol_key.to_uppercase();
            if let Some(protocol) = state.protocols.get(&protocol_key) {
                state
                    .database
                    .lp_pool_state
                    .get_supplied_borrowed_series_with_window(
                        protocol.contracts.lpp.to_owned(),
                        months,
                        query.from,
                    )
                    .await?
            } else {
                vec![]
            }
        } else {
            let protocols: Vec<String> = state
                .protocols
                .values()
                .map(|p| p.contracts.lpp.to_owned())
                .collect();

            state
                .database
                .lp_pool_state
                .get_supplied_borrowed_series_total_with_window(
                    protocols, months, query.from,
                )
                .await?
        };
        Ok(data)
    };

    let data = if query.from.is_none() {
        cached_fetch(
            &state.api_cache.supplied_borrowed_history,
            &cache_key,
            fetch,
        )
        .await?
    } else {
        fetch().await?
    };

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "supplied-borrowed-history.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}

// =============================================================================
// Monthly Active Wallets
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct MonthlyActiveWalletsQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/monthly-active-wallets")]
pub async fn monthly_active_wallets(
    state: web::Data<AppState<State>>,
    query: web::Query<MonthlyActiveWalletsQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("monthly_active_wallets", period_str, query.from);

    let fetch = || async {
        let data = state
            .database
            .ls_opening
            .get_monthly_active_wallets_with_window(months, query.from)
            .await?;
        let wallets: Vec<MonthlyActiveWallet> = data
            .into_iter()
            .map(|w| MonthlyActiveWallet {
                month: w.month,
                unique_addresses: w.unique_addresses,
            })
            .collect();
        Ok(wallets)
    };

    let data = if query.from.is_none() {
        cached_fetch(&state.api_cache.monthly_active_wallets, &cache_key, fetch)
            .await?
    } else {
        fetch().await?
    };

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&data, "monthly-active-wallets.csv"),
        _ => Ok(HttpResponse::Ok().json(data)),
    }
}
