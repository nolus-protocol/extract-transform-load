use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/total-tx-value")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    state
        .database
        .ls_opening
        .get_total_tx_value()
        .await
        .map(|total_tx_value| web::Json(Response { total_tx_value }))
        .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_tx_value: BigDecimal,
}
