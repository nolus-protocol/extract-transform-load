use actix_web::{get, web, Responder};
use bigdecimal::{BigDecimal, Zero as _};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/total-value-locked")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    Ok(web::Json(Response {
        total_value_locked: state
            .cache
            .lock()
            .ok()
            .and_then(|item| item.total_value_locked.clone())
            .unwrap_or_else(BigDecimal::zero),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_value_locked: BigDecimal,
}
