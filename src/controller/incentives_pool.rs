use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/incentives-pool")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.tr_state.get_incentives_pool().await?;
    Ok(web::Json(Response {
        incentives_pool: data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub incentives_pool: BigDecimal,
}
