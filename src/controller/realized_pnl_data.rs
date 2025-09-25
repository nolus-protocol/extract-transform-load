use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

#[get("/realized-pnl-data")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_opening
        .get_realized_pnl_data(address)
        .await?;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
