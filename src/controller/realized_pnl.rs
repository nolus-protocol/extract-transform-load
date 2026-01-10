use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/realized-pnl",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the total realized profit and loss for a specific wallet address.", body = Response)
    )
)]
#[get("/realized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_loan_closing
        .get_realized_pnl(address)
        .await?;
    Ok(web::Json(Response { realized_pnl: data }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total realized PnL in USD
    pub realized_pnl: f64,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}
