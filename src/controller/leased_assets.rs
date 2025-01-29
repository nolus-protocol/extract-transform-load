use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/leased-assets")]
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
                .get_leased_assets(protocol.contracts.lpp.to_owned())
                .await?;
            return Ok(web::Json(data));
        }
    }

    let data = state.database.ls_opening.get_leased_assets_total().await?;
    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
