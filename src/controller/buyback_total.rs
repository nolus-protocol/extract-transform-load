use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};
use bigdecimal::BigDecimal;
use serde::Serialize;

use crate::{configuration::State, error::Error};

#[get("/buyback-total")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .tr_profit
        .get_buyback_total()
        .await
        .map(|buyback_total| Json(Response { buyback_total }))
        .map_err(From::from)
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub buyback_total: BigDecimal,
}
