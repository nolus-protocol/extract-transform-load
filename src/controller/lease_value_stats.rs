use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const CACHE_KEY: &str = "lease_value_stats";

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Response format
    #[param(inline, value_type = Option<String>)]
    format: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/lease-value-stats",
    tag = "Lending Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns statistical aggregates (min, max, avg, sum) of lease values per protocol. Cache: 1 hour.", body = Vec<LeaseValueStatResponse>)
    )
)]
#[get("/lease-value-stats")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.lease_value_stats.get(CACHE_KEY).await {
        let stats: Vec<LeaseValueStat> = cached
            .into_iter()
            .map(|s| LeaseValueStat {
                asset: s.asset,
                avg_value: s.avg_value,
                max_value: s.max_value,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&stats, "lease-value-stats.csv"),
            _ => Ok(HttpResponse::Ok().json(stats)),
        };
    }

    // Cache miss - query DB
    let data = state.database.ls_state.get_lease_value_stats().await?;

    // Store in cache
    state.api_cache.lease_value_stats.set(CACHE_KEY, data.clone()).await;

    let stats: Vec<LeaseValueStat> = data
        .into_iter()
        .map(|s| LeaseValueStat {
            asset: s.asset,
            avg_value: s.avg_value,
            max_value: s.max_value,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&stats, "lease-value-stats.csv"),
        _ => Ok(HttpResponse::Ok().json(stats)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseValueStat {
    pub asset: String,
    pub avg_value: BigDecimal,
    pub max_value: BigDecimal,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LeaseValueStatResponse {
    /// Asset symbol
    pub asset: String,
    /// Average lease value in USD
    #[schema(value_type = f64)]
    pub avg_value: BigDecimal,
    /// Maximum lease value in USD
    #[schema(value_type = f64)]
    pub max_value: BigDecimal,
}
