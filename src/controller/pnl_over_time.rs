use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[utoipa::path(
    get,
    path = "/api/pnl-over-time",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns PnL progression over time for a specific wallet address.", body = Vec<PnlOverTimeResponse>)
    )
)]
#[get("/pnl-over-time")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let mut interval = data.interval;

    if interval > 30 {
        interval = 30;
    }

    let data = state
        .database
        .ls_state
        .get_pnl_over_time(data.address.to_owned(), interval)
        .await?;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Number of days to look back (max 30)
    interval: i64,
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PnlOverTimeResponse {
    /// Date of the data point
    pub date: DateTime<Utc>,
    /// PnL value in USD
    #[schema(value_type = f64)]
    pub pnl: BigDecimal,
}
