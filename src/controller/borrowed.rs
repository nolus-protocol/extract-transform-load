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
    path = "/api/borrowed",
    tag = "Protocol Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the total amount currently borrowed across all lending pools in USD. Cache: 30 min.", body = Response)
    )
)]
#[get("/borrowed")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    if let Some(protocolKey) = &data.protocol {
        let protocolKey = protocolKey.to_uppercase();
        let admin = state.protocols.get(&protocolKey);

        if let Some(protocol) = admin {
            let data = state
                .database
                .ls_opening
                .get_borrowed(protocol.contracts.lpp.to_owned())
                .await?;
            return Ok(web::Json(Response { borrowed: data }));
        }
    }

    let data = state.database.ls_opening.get_borrowed_total().await?;
    Ok(web::Json(Response { borrowed: data }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total borrowed amount in USD
    #[schema(value_type = f64)]
    pub borrowed: BigDecimal,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Filter by protocol (e.g., OSMOSIS-OSMOSIS-USDC)
    protocol: Option<String>,
}
