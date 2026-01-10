use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/leases",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns a paginated list of leases for a specific wallet address.", body = Vec<LeaseResponse>)
    )
)]
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Number of records to skip (default: 0)
    skip: Option<i64>,
    /// Maximum number of records to return (default: 10, max: 10)
    limit: Option<i64>,
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LeaseResponse {
    /// Lease contract ID
    pub lease: String,
    /// Lease status
    pub status: String,
    /// Opening date
    pub opened: DateTime<Utc>,
    /// Closing date (if closed)
    pub closed: Option<DateTime<Utc>>,
    /// Profit/Loss in USD
    #[schema(value_type = f64)]
    pub pnl: BigDecimal,
}
