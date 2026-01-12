use std::str::FromStr as _;

use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

/// DEPRECATED: Use /api/pools endpoint instead, which includes earn_apr for all pools.
/// This endpoint will be removed in a future version.
#[get("/earn-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let earn_apr = if let Some(protocol_key) = &data.protocol {
        let protocol_key = protocol_key.to_uppercase();
        let admin = state.protocols.get(&protocol_key);
        if let Some(protocol) = admin {
            match protocol_key.as_str() {
                "OSMOSIS-OSMOSIS-ALL_BTC" | "OSMOSIS-OSMOSIS-ATOM" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        2.5,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                "OSMOSIS-OSMOSIS-ALL_SOL" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        4.0,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                "OSMOSIS-OSMOSIS-ST_ATOM" | "OSMOSIS-OSMOSIS-AKT" => state
                    .database
                    .ls_opening
                    .get_earn_apr_interest(
                        protocol.contracts.lpp.to_owned(),
                        2.0,
                    )
                    .await
                    .unwrap_or(BigDecimal::from(0)),
                _ => state
                    .database
                    .ls_opening
                    .get_earn_apr(protocol.contracts.lpp.to_owned())
                    .await
                    .unwrap_or(BigDecimal::from(0)),
            }
        } else {
            BigDecimal::from_str("0")?
        }
    } else {
        BigDecimal::from_str("0")?
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Deprecation", "true"))
        .insert_header(("Sunset", "2025-06-01"))
        .insert_header(("Link", "</api/pools>; rel=\"successor-version\""))
        .json(Response { earn_apr }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub earn_apr: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
