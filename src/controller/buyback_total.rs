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
    state
        .database
        .tr_profit
        .get_buyback_total()
        .await
        .map(|buyback_total| web::Json(Response { buyback_total }))
        .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub buyback_total: BigDecimal,
}
