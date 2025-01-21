use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{configuration::State, error::Error};

#[get("/incentives-pool")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .tr_state
        .get_incentives_pool()
        .await
        .map(|incentives_pool| Json(Response { incentives_pool }))
        .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub incentives_pool: BigDecimal,
}
