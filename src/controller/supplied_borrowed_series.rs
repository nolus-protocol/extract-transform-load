use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/time-series")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query { protocol }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    if let Some(protocol) = protocol {
        state
            .database
            .lp_pool_state
            .get_supplied_borrowed_series(protocol.contracts.lpp.clone())
            .await
    } else {
        state
            .database
            .lp_pool_state
            .get_supplied_borrowed_series_total(
                state
                    .protocols
                    .values()
                    .map(|protocol| protocol.contracts.lpp.clone())
                    .collect(),
            )
            .await
    }
    .map(web::Json)
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
