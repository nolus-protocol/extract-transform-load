use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub borrowed: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
