use actix_web::{
    error::ErrorInternalServerError,
    get,
    web::{Data, Json},
    Responder, Result,
};
use bigdecimal::{BigDecimal, Zero as _};
use serde::{Deserialize, Serialize};

use crate::configuration::State;

#[get("/total-value-locked")]
async fn index(state: Data<State>) -> Result<impl Responder> {
    state
        .cache
        .read()
        .map(|lock| {
            Json(Response {
                total_value_locked: lock
                    .total_value_locked
                    .clone()
                    .unwrap_or_else(BigDecimal::zero),
            })
        })
        .map_err(ErrorInternalServerError)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_value_locked: BigDecimal,
}
