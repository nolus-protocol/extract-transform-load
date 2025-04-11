use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

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

#[derive(Debug, Deserialize)]
pub struct Query {
    interval: i64,
    address: String,
}
