use actix_web::{get, web, Responder};
use bigdecimal::{BigDecimal, Zero as _};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/earn-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query { protocol }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol).map(|protocol_data| {
            (
                protocol_data,
                match &*protocol {
                    "OSMOSIS-OSMOSIS-ALL_BTC" => Some(15),
                    "OSMOSIS-OSMOSIS-ALL_SOL" | "OSMOSIS-OSMOSIS-АКТ" => {
                        Some(20)
                    },
                    _ => None,
                },
            )
        })
    });

    Ok(web::Json(Response {
        earn_apr: if let Some((protocol, max_interest)) = protocol {
            let protocol = protocol.contracts.lpp.to_owned();

            if let Some(max_interest) = max_interest {
                state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(protocol, max_interest)
                    .await
            } else {
                state.database.ls_opening.get_earn_apr(protocol).await
            }
            .ok()
        } else {
            None
        }
        .unwrap_or_else(BigDecimal::zero),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub earn_apr: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
