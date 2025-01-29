use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/leases")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(10);

    if limit > 10 {
        limit = 10;
    }

    let address = data.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_opening
        .get_leases_by_address(address, skip, limit)
        .await?;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
}
