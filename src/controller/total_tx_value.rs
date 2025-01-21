use actix_web::{
    get,
    web::{Data, Json},
    Responder, Result,
};
use bigdecimal::BigDecimal;
use serde::Serialize;

use crate::{configuration::State, error::Error};

#[get("/total-tx-value")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .ls_opening
        .get_total_tx_value()
        .await
        .map(|total_tx_value| Json(Response { total_tx_value }))
        .map_err(From::from)
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub total_tx_value: BigDecimal,
}
