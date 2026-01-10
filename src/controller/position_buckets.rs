use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::PositionBucket,
};

const CACHE_KEY: &str = "position_buckets";

#[utoipa::path(
    get,
    path = "/api/position-buckets",
    tag = "Position Analytics",
    responses(
        (status = 200, description = "Returns position count and size distribution across predefined loan size buckets. Cache: 1 hour.", body = Vec<PositionBucketResponse>)
    )
)]
#[get("/position-buckets")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.position_buckets.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state.database.ls_state.get_position_buckets().await?;
    let buckets: Vec<PositionBucket> = data
        .into_iter()
        .map(|b| PositionBucket {
            loan_category: b.loan_category.unwrap_or_default(),
            loan_count: b.loan_count,
            loan_size: b.loan_size,
        })
        .collect();

    state.api_cache.position_buckets.set(CACHE_KEY, buckets.clone()).await;

    Ok(web::Json(buckets))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PositionBucketResponse {
    /// Loan size category (e.g., "$1k-$5k")
    pub loan_category: String,
    /// Number of loans in this category
    pub loan_count: i64,
    /// Total loan size in USD
    #[schema(value_type = f64)]
    pub loan_size: BigDecimal,
}
