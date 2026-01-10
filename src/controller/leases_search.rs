use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/leases-search",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Searches and returns leases associated with a specific wallet address.", body = Vec<String>)
    )
)]
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Number of records to skip (default: 0)
    skip: Option<i64>,
    /// Maximum number of records to return (default: 10, max: 100)
    limit: Option<i64>,
    /// Wallet address
    address: String,
    /// Search term
    search: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LeaseSearchResponse(
    /// Lease contract ID
    pub String,
);
