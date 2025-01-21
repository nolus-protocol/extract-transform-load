use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};
use bigdecimal::BigDecimal;
use serde::Serialize;

use crate::{configuration::State, error::Error};

#[get("/distributed")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .tr_rewards_distribution
        .get_distributed()
        .await
        .map(|distributed| Json(Response { distributed }))
        .map_err(From::from)
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub distributed: BigDecimal,
}
