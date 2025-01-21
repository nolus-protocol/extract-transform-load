use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, error::Error};

#[get("/time-series")]
async fn index(
    state: Data<State>,
    Query(Arguments { mut protocol }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    if let Some(protocol) = protocol {
        state
            .database
            .lp_pool_state
            .get_supplied_borrowed_series(&protocol.contracts.lpp)
            .await
    } else {
        state
            .database
            .lp_pool_state
            .get_supplied_borrowed_series_total(
                state
                    .protocols
                    .values()
                    .map(|protocol| &protocol.contracts.lpp),
            )
            .await
    }
    .map(Json)
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    protocol: Option<String>,
}
