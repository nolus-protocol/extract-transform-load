use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{build_cache_key, parse_period_months, to_csv_response, to_streaming_csv_response},
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
    /// Only return records after this timestamp (exclusive), for incremental syncing
    from: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InterestRepayment {
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
    pub position_owner: String,
    pub position_type: String,
    pub event_type: String,
    pub loan_interest_repaid: BigDecimal,
    pub margin_interest_repaid: BigDecimal,
}

impl From<crate::dao::postgre::ls_repayment::InterestRepaymentData> for InterestRepayment {
    fn from(r: crate::dao::postgre::ls_repayment::InterestRepaymentData) -> Self {
        Self {
            timestamp: r.timestamp,
            contract_id: r.contract_id,
            position_owner: r.position_owner,
            position_type: r.position_type,
            event_type: r.event_type,
            loan_interest_repaid: r.loan_interest_repaid,
            margin_interest_repaid: r.margin_interest_repaid,
        }
    }
}

#[get("/interest-repayments")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("12m");
    let cache_key = build_cache_key("interest_repayments", period_str, query.from);

    // Try cache first
    if let Some(cached) = state.api_cache.interest_repayments.get(&cache_key).await {
        let data: Vec<InterestRepayment> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "interest-repayments.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_repayment
        .get_interest_repayments_with_window(months, query.from)
        .await?;

    // Store in cache
    state.api_cache.interest_repayments.set(&cache_key, data.clone()).await;

    let response: Vec<InterestRepayment> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "interest-repayments.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[get("/interest-repayments/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "interest_repayments_all";

    if let Some(cached) = state.api_cache.interest_repayments.get(CACHE_KEY).await {
        let data: Vec<InterestRepayment> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "interest-repayments.csv");
    }

    let data = state
        .database
        .ls_repayment
        .get_interest_repayments_with_window(None, None)
        .await?;

    state.api_cache.interest_repayments.set(CACHE_KEY, data.clone()).await;

    let response: Vec<InterestRepayment> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "interest-repayments.csv")
}
