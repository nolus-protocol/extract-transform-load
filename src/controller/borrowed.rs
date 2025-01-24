use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/borrowed")]
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
            .get_borrowed(protocol.contracts.lpp.clone())
            .await
    } else {
        state.database.ls_opening.get_borrowed_total().await
    }
    .map(|borrowed| web::Json(Response { borrowed }))
    .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub borrowed: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    protocol: Option<String>,
}
