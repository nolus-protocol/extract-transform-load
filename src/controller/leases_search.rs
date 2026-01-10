use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/leases-search")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(10);

    if limit > 100 {
        limit = 100;
    }

    let address = data.address.to_lowercase().to_owned();
    let search = data.search.to_owned();

    let data = state
        .database
        .ls_opening
        .get_leases_addresses(address, search, skip, limit)
        .await?;
    let data: Vec<String> = data.iter().map(|e| e.0.to_owned()).collect();

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
    search: Option<String>,
}
