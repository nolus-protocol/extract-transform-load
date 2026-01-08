use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
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
    let cache_key = format!("interest_repayments_{}_{}", skip, limit);

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
        .get_interest_repayments(skip, limit)
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
