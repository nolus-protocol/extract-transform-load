use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    format: Option<String>,
}

#[get("/historical-lenders")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let cache_key = format!("historical_lenders_{}_{}", skip, limit);

    // Try cache first
    if let Some(cached) = state.api_cache.historical_lenders.get(&cache_key).await {
        let lenders: Vec<HistoricalLender> = cached
            .into_iter()
            .map(|l| HistoricalLender {
                transaction_type: l.transaction_type,
                timestamp: l.timestamp,
                user: l.user,
                amount: l.amount,
                pool: l.pool,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&lenders, "historical-lenders.csv"),
            _ => Ok(HttpResponse::Ok().json(lenders)),
        };
    }

    // Cache miss - query DB
    let data = state.database.lp_deposit.get_historical_lenders(skip, limit).await?;

    // Store in cache
    state.api_cache.historical_lenders.set(&cache_key, data.clone()).await;

    let lenders: Vec<HistoricalLender> = data
        .into_iter()
        .map(|l| HistoricalLender {
            transaction_type: l.transaction_type,
            timestamp: l.timestamp,
            user: l.user,
            amount: l.amount,
            pool: l.pool,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&lenders, "historical-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(lenders)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}
