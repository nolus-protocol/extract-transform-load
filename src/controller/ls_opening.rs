use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

#[get("/ls-opening")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let result = state.database.ls_opening.get(data.lease.to_owned()).await?;
    Ok(web::Json(result))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    lease: Option<String>
}
