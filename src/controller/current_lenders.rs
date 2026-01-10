use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const CACHE_KEY: &str = "current_lenders";

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/current-lenders",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns a list of currently active lenders with their deposit amounts per pool. Cache: 1 hour.", body = Vec<CurrentLenderResponse>)
    )
)]
#[get("/current-lenders")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.current_lenders.get(CACHE_KEY).await {
        let lenders: Vec<Lender> = cached
            .into_iter()
            .map(|l| Lender {
                joined: l.joined,
                pool: l.pool,
                lender: l.lender,
                lent_stables: l.lent_stables,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&lenders, "current-lenders.csv"),
            _ => Ok(HttpResponse::Ok().json(lenders)),
        };
    }

    // Cache miss - query DB
    let data = state.database.lp_lender_state.get_current_lenders().await?;

    // Store in cache
    state.api_cache.current_lenders.set(CACHE_KEY, data.clone()).await;

    let lenders: Vec<Lender> = data
        .into_iter()
        .map(|l| Lender {
            joined: l.joined,
            pool: l.pool,
            lender: l.lender,
            lent_stables: l.lent_stables,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&lenders, "current-lenders.csv"),
        _ => Ok(HttpResponse::Ok().json(lenders)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Lender {
    pub joined: Option<DateTime<Utc>>,
    pub pool: Option<String>,
    pub lender: String,
    pub lent_stables: BigDecimal,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CurrentLenderResponse {
    /// Date when the lender joined
    pub joined: Option<DateTime<Utc>>,
    /// Pool name
    pub pool: Option<String>,
    /// Lender wallet address
    pub lender: String,
    /// Amount lent in stables
    #[schema(value_type = f64)]
    pub lent_stables: BigDecimal,
}
