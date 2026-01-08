use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 5000;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    /// Filter to only return repayments after this timestamp (exclusive)
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

#[get("/interest-repayments")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let from = query.from;

    // Build cache key including the 'from' parameter
    let from_key = from
        .map(|ts| ts.timestamp().to_string())
        .unwrap_or_else(|| "none".to_string());
    let cache_key =
        format!("interest_repayments_{}_{}_{}", skip, limit, from_key);

    // Try cache first
    if let Some(cached) =
        state.api_cache.interest_repayments.get(&cache_key).await
    {
        let repayments: Vec<InterestRepayment> = cached
            .into_iter()
            .map(|r| InterestRepayment {
                timestamp: r.timestamp,
                contract_id: r.contract_id,
                position_owner: r.position_owner,
                position_type: r.position_type,
                event_type: r.event_type,
                loan_interest_repaid: r.loan_interest_repaid,
                margin_interest_repaid: r.margin_interest_repaid,
            })
            .collect();
        return Ok(HttpResponse::Ok().json(repayments));
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_repayment
        .get_interest_repayments(skip, limit, from)
        .await?;

    // Store in cache
    state
        .api_cache
        .interest_repayments
        .set(&cache_key, data.clone())
        .await;

    let repayments: Vec<InterestRepayment> = data
        .into_iter()
        .map(|r| InterestRepayment {
            timestamp: r.timestamp,
            contract_id: r.contract_id,
            position_owner: r.position_owner,
            position_type: r.position_type,
            event_type: r.event_type,
            loan_interest_repaid: r.loan_interest_repaid,
            margin_interest_repaid: r.margin_interest_repaid,
        })
        .collect();

    Ok(HttpResponse::Ok().json(repayments))
}
