use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use bigdecimal::{BigDecimal, Zero};
use serde::{Deserialize, Serialize};

use crate::{configuration::State, custom_uint::UInt31, error::Error};

#[get("/earn-apr")]
async fn index(
    state: Data<State>,
    Query(Arguments { mut protocol }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    protocol.make_ascii_uppercase();

    let max_interest = match &*protocol {
        "OSMOSIS-OSMOSIS-ALL_BTC" => {
            const { Some(UInt31::from_unsigned(15).unwrap()) }
        },
        "OSMOSIS-OSMOSIS-ALL_SOL" | "OSMOSIS-OSMOSIS-АКТ" => {
            const { Some(UInt31::from_unsigned(20).unwrap()) }
        },
        _ => None,
    };

    let Some(protocol) = state.protocols.get(&protocol) else {
        return Ok(Json(Response {
            earn_apr: BigDecimal::zero(),
        }));
    };

    if let Some(max_interest) = max_interest {
        state
            .database
            .ls_opening
            .get_earn_apr_interest(&protocol.contracts.lpp, max_interest)
            .await
    } else {
        state
            .database
            .ls_opening
            .get_earn_apr(&protocol.contracts.lpp)
            .await
    }
    .map(|earn_apr| Json(Response { earn_apr }))
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    protocol: String,
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub earn_apr: BigDecimal,
}
