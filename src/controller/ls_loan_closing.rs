use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/ls-loan-closing")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query {
        skip,
        limit,
        address,
    }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    state
        .database
        .ls_loan_closing
        .get_leases(
            address,
            skip.unwrap_or(0),
            limit.map_or(10, |limit| limit.min(10)),
        )
        .await
        .map(web::Json)
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
}
