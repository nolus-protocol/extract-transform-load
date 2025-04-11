use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

#[get("/unrealized-pnl-by-address")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let items = state
        .database
        .ls_state
        .get_unrealized_pnl_by_address(data.address.to_owned())
        .await?;

    Ok(web::Json(items))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
