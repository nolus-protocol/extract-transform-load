use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/utilization-level")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query {
        skip,
        limit,
        protocol,
    }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = skip.unwrap_or(0);

    let limit = limit.map_or(32, |limit| limit.min(100));

    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    if let Some(protocol) = protocol {
        state
            .database
            .lp_pool_state
            .get_utilization_level(
                protocol.contracts.lpp.to_owned(),
                skip,
                limit,
            )
            .await
    } else {
        state
            .database
            .lp_pool_state
            .get_utilization_level_old(skip, limit)
            .await
    }
    .map(|item| {
        web::Json(
            item.into_iter()
                .map(|item| item.utilization_level)
                .collect::<Vec<_>>(),
        )
    })
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    protocol: Option<String>,
}
