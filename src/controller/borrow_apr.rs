use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/borrow-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query {
        skip,
        limit,
        protocol,
    }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    let Some(protocol) = protocol else {
        return Ok(web::Json(vec![]));
    };

    state
        .database
        .ls_opening
        .get_borrow_apr(
            protocol.contracts.lpp.clone(),
            skip.unwrap_or(0),
            limit.map_or(32, |limit| limit.min(100)),
        )
        .await
        .map(|aprs| {
            web::Json(aprs.into_iter().map(|apr| apr.APR).collect::<Vec<_>>())
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    protocol: Option<String>,
}
