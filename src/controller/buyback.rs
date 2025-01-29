use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/buyback")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query { skip, limit }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    state
        .database
        .tr_profit
        .get_buyback(
            skip.unwrap_or(0),
            limit.map_or(32, |limit| limit.min(100)),
        )
        .await
        .map(web::Json)
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
}
