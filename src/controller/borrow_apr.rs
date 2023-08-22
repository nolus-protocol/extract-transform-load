use crate::{
    configuration::{AppState, State},
    error::Error, model::Borrow_APR,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/borrow-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let limit = data.limit.unwrap_or(32);
    let data = state.database.ls_opening.get_borrow_apr(skip, limit).await?;
    Ok(web::Json(Response { result: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: Vec<Borrow_APR>,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
}