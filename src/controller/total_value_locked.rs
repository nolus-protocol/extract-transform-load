use std::str::FromStr as _;

use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/total-value-locked")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let total_value_locked = if let Ok(item) = state.cache.lock() {
        item.total_value_locked
            .to_owned()
            .unwrap_or(BigDecimal::from_str("0")?)
    } else {
        BigDecimal::from_str("0")?
    };

    Ok(web::Json(Response { total_value_locked }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub total_value_locked: BigDecimal,
}
