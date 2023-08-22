use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/yield")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.lp_deposit.get_yield().await?;
    Ok(web::Json(Response { r#yield: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub r#yield: BigDecimal,
}
