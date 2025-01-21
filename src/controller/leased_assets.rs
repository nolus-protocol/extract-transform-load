use std::borrow::Cow;

use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, error::Error};

#[get("/leased-assets")]
async fn index(
    state: Data<State>,
    Query(Arguments { protocol }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    if let Some(mut protocol) = protocol {
        protocol.make_ascii_uppercase();

        let protocol = state
            .protocols
            .get(&protocol)
            .ok_or_else(|| Error::ProtocolError(Cow::Owned(protocol)))?;

        state
            .database
            .ls_opening
            .get_leased_assets(&protocol.contracts.lpp)
            .await
    } else {
        state.database.ls_opening.get_leased_assets_total().await
    }
    .map(Json)
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    protocol: Option<String>,
}
