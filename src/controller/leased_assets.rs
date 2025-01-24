use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/leased-assets")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query { protocol }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let protocol = protocol.and_then(|mut protocol| {
        protocol.make_ascii_uppercase();

        state.protocols.get(&protocol)
    });

    if let Some(protocol) = protocol {
        state
            .database
            .ls_opening
            .get_leased_assets(protocol.contracts.lpp.to_owned())
            .await
    } else {
        state.database.ls_opening.get_leased_assets_total().await
    }
    .map(web::Json)
    .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
