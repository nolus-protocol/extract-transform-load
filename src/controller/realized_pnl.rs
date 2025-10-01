use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub realized_pnl: f64,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
