use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response},
    model::MonthlyActiveWallet,
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
}

#[get("/monthly-active-wallets")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("monthly_active_wallets", period_str, query.from);

    // Try cache first (only if no 'from' filter)
    if query.from.is_none() {
        if let Some(cached) = state.api_cache.monthly_active_wallets.get(&cache_key).await {
            return match query.format.as_deref() {
                Some("csv") => to_csv_response(&cached, "monthly-active-wallets.csv"),
                _ => Ok(HttpResponse::Ok().json(cached)),
            };
        }
    }

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

    // Store in cache (only if no 'from' filter)
    if query.from.is_none() {
        state.api_cache.monthly_active_wallets.set(&cache_key, wallets.clone()).await;
    }

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&wallets, "monthly-active-wallets.csv"),
        _ => Ok(HttpResponse::Ok().json(wallets)),
    }
}
