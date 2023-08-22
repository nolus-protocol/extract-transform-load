use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Utilization_Level,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/utilization-level")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let limit = data.limit.unwrap_or(32);
    let data = state
        .database
        .lp_pool_state
        .get_utilization_level(skip, limit)
        .await?;
    Ok(web::Json(Response { result: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: Vec<Utilization_Level>,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
}
