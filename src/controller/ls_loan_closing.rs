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
    path = "/api/ls-loan-closing",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns paginated lease closing details with PnL for a specific wallet.", body = Vec<LsLoanClosingResponse>)
    )
)]
#[get("/ls-loan-closing")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(10);

    if limit > 10 {
        limit = 10;
    }

    let items = state
        .database
        .ls_loan_closing
        .get_leases(data.address.to_owned(), skip, limit)
        .await?;

    Ok(web::Json(items))
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
pub struct LsLoanClosingResponse {
    /// Lease contract ID
    pub lease: String,
    /// Profit/Loss in USD
    #[schema(value_type = f64)]
    pub pnl: BigDecimal,
    /// Closing timestamp
    pub closed_at: DateTime<Utc>,
}
