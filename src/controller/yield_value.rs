use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/yield")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let r#yield = if let Ok(item) = state.cache.lock() {
        item.r#yield.to_owned().unwrap_or(BigDecimal::from_str("0")?)
    }else{
        BigDecimal::from_str("0")?
    };

    Ok(web::Json(Response { r#yield }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub r#yield: BigDecimal,
}
