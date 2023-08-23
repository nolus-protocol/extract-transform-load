use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Buyback,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

#[get("/buyback")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(32);

    if limit > 100 {
        limit = 100;
    }

    let data: Vec<Buyback> = state.database.tr_profit.get_buyback(skip, limit).await?;
    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
}
