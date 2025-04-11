use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/open-position-value")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.ls_state.get_open_position_value().await?;
    Ok(web::Json(ResponseData {
        open_position_value: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub open_position_value: BigDecimal,
}
