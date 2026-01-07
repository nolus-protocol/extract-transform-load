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
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    format: Option<String>,
}

#[get("/liquidations")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let cache_key = format!("liquidations_{}_{}", skip, limit);

    // Try cache first
    if let Some(cached) = state.api_cache.liquidations.get(&cache_key).await {
        let liquidations: Vec<Liquidation> = cached
            .into_iter()
            .map(|l| Liquidation {
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
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&liquidations, "liquidations.csv"),
            _ => Ok(HttpResponse::Ok().json(liquidations)),
        };
    }

    // Cache miss - query DB
    let data = state.database.ls_liquidation.get_liquidations(skip, limit).await?;

    // Store in cache
    state.api_cache.liquidations.set(&cache_key, data.clone()).await;

    let liquidations: Vec<Liquidation> = data
        .into_iter()
        .map(|l| Liquidation {
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
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&liquidations, "liquidations.csv"),
        _ => Ok(HttpResponse::Ok().json(liquidations)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Liquidation {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub contract_id: String,
    pub user: Option<String>,
    pub transaction_type: Option<String>,
    pub liquidation_amount: BigDecimal,
    pub closed_loan: bool,
    pub down_payment: BigDecimal,
    pub loan: BigDecimal,
    pub liquidation_price: Option<BigDecimal>,
}
