use std::str::FromStr as _;

use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/earn-apr",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the current earn APR for liquidity providers, optionally filtered by protocol.", body = Response)
    )
)]
#[get("/earn-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    if let Some(protocolKey) = &data.protocol {
        let protocolKey = protocolKey.to_uppercase();
        let admin = state.protocols.get(&protocolKey);
        if let Some(protocol) = admin {
            let data = match protocolKey.as_str() {
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
            };
            return Ok(web::Json(Response { earn_apr: data }));
        }
    }

    Ok(web::Json(Response {
        earn_apr: BigDecimal::from_str("0")?,
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Earn APR percentage
    #[schema(value_type = f64)]
    pub earn_apr: BigDecimal,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Protocol identifier (e.g., OSMOSIS-OSMOSIS-USDC)
    protocol: Option<String>,
}
