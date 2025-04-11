use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/supplied-funds")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.lp_pool_state.get_supplied_funds().await?;

    Ok(web::Json(ResponseData {
        amount: data.with_scale(2),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub amount: BigDecimal,
}
