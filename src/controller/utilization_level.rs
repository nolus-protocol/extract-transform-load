use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, custom_uint::UInt63, error::Error};

#[get("/utilization-level")]
async fn index(
    state: Data<State>,
    Query(Arguments {
        skip,
        limit,
        protocol,
    }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    let skip = skip.unwrap_or(const { UInt63::from_unsigned(0).unwrap() });

    let limit = limit
        .map_or(const { UInt63::from_unsigned(32).unwrap() }, |limit| {
            limit.min(const { UInt63::from_unsigned(100).unwrap() })
        });

    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    if let Some(protocol) = protocol {
        state
            .database
            .lp_pool_state
            .get_utilization_level(&protocol.contracts.lpp, skip, limit)
            .await
    } else {
        state
            .database
            .lp_pool_state
            .get_utilization_level_old(skip, limit)
            .await
    }
    .map(|levels| {
        Json(
            levels
                .into_iter()
                .map(|level| level.utilization_level)
                .collect(),
        )
    })
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    skip: Option<UInt63>,
    limit: Option<UInt63>,
    protocol: Option<String>,
}
