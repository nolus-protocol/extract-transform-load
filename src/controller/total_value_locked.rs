use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/total-value-locked")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.ls_state.get_total_value_locked().await?;
    Ok(web::Json(Response { total_value_locked: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_value_locked: BigDecimal,
}
