use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{configuration::State, error::Error};

#[get("/revenue")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .tr_profit
        .get_revenue()
        .await
        .map(|revenue| Json(Response { revenue }))
        .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub revenue: BigDecimal,
}
