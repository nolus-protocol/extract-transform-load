use crate::{
    configuration::{AppState, State},
    error::Error, model::Supplied_Borrowed_Series,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/time-series")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.lp_pool_state.get_supplied_borrowed_series().await?;
    Ok(web::Json(Response { result: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: Vec<Supplied_Borrowed_Series>,
}
