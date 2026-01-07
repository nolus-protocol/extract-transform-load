use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::PositionBucket,
};

const CACHE_KEY: &str = "position_buckets";

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
