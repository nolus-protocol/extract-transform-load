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

    let mut protocols: Vec<String> = vec![];

    for  data in state.protocols.values() {
        protocols.push(data.contracts.lpp.to_owned());
    }

    let data = state
        .database
        .lp_pool_state
        .get_supplied_borrowed_series_total(protocols)
        .await?;
    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
