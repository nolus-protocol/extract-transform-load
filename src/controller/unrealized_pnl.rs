use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/unrealized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.ls_state.get_unrealized_pnl().await?;
    Ok(web::Json(ResponseData {
        unrealized_pnl: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub unrealized_pnl: BigDecimal,
}
