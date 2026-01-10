use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const CACHE_KEY: &str = "loans_granted";

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/loans-granted",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns a summary of loans granted per protocol with count and total value. Cache: 1 hour.", body = Vec<LoanGrantedResponse>)
    )
)]
#[get("/loans-granted")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.loans_granted.get(CACHE_KEY).await {
        let loans: Vec<LoanGranted> = cached
            .into_iter()
            .map(|l| LoanGranted {
                asset: l.asset,
                loan: l.loan,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&loans, "loans-granted.csv"),
            _ => Ok(HttpResponse::Ok().json(loans)),
        };
    }

    // Cache miss - query DB
    let data = state.database.ls_opening.get_loans_granted().await?;

    // Store in cache
    state.api_cache.loans_granted.set(CACHE_KEY, data.clone()).await;

    let loans: Vec<LoanGranted> = data
        .into_iter()
        .map(|l| LoanGranted {
            asset: l.asset,
            loan: l.loan,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&loans, "loans-granted.csv"),
        _ => Ok(HttpResponse::Ok().json(loans)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoanGranted {
    pub asset: String,
    pub loan: BigDecimal,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LoanGrantedResponse {
    /// Asset symbol
    pub asset: String,
    /// Total loan amount in USD
    #[schema(value_type = f64)]
    pub loan: BigDecimal,
}
