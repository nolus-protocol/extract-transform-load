use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/revenue")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    state
        .database
        .tr_profit
        .get_revenue()
        .await
        .map(|revenue| web::Json(Response { revenue }))
        .map_err(From::from)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub revenue: BigDecimal,
}
