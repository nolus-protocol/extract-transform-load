use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/buyback-total")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.tr_profit.get_buyback_total().await?;
    Ok(web::Json(Response {
        buyback_total: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub buyback_total: BigDecimal,
}
