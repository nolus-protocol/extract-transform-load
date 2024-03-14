use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/total-tx-value")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.ls_opening.get_total_tx_value().await?;
    Ok(web::Json(Response { total_tx_value: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_tx_value: BigDecimal,
}
