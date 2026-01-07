use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const CACHE_KEY: &str = "historically_repaid";

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
}

#[get("/historically-repaid")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.historically_repaid.get(CACHE_KEY).await {
        let repaid: Vec<HistoricallyRepaid> = cached
            .into_iter()
            .map(|r| HistoricallyRepaid {
                contract_id: r.contract_id,
                symbol: r.symbol,
                loan: r.loan,
                total_repaid: r.total_repaid,
                close_timestamp: r.close_timestamp,
                loan_closed: r.loan_closed,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&repaid, "historically-repaid.csv"),
            _ => Ok(HttpResponse::Ok().json(repaid)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_repayment
        .get_historically_repaid()
        .await?;

    // Store in cache
    state.api_cache.historically_repaid.set(CACHE_KEY, data.clone()).await;

    let repaid: Vec<HistoricallyRepaid> = data
        .into_iter()
        .map(|r| HistoricallyRepaid {
            contract_id: r.contract_id,
            symbol: r.symbol,
            loan: r.loan,
            total_repaid: r.total_repaid,
            close_timestamp: r.close_timestamp,
            loan_closed: r.loan_closed,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&repaid, "historically-repaid.csv"),
        _ => Ok(HttpResponse::Ok().json(repaid)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyRepaid {
    pub contract_id: String,
    pub symbol: String,
    pub loan: BigDecimal,
    pub total_repaid: BigDecimal,
    pub close_timestamp: Option<DateTime<Utc>>,
    pub loan_closed: String,
}
