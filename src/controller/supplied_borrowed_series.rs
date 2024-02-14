use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::Deserialize;

#[get("/time-series")]
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
                .lp_pool_state
                .get_supplied_borrowed_series(protocol.contracts.lpp.to_owned())
                .await?;
            return Ok(web::Json(data));
        }
    }

    let osmosis = if let Some(osmosis) = state.protocols.get("OSMOSIS") {
        osmosis
    } else {
        return Err(Error::ProtocolError(String::from("osmosis")));
    };

    let neutron = if let Some(neutron) = state.protocols.get("NEUTRON") {
        neutron
    } else {
        return Err(Error::ProtocolError(String::from("neutron")));
    };

    let data = state
        .database
        .lp_pool_state
        .get_supplied_borrowed_series_total(
            osmosis.contracts.lpp.to_owned(),
            neutron.contracts.lpp.to_owned(),
        )
        .await?;
    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
