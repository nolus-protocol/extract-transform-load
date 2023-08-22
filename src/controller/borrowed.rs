use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/borrowed")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.lp_pool_state.get_borrowed().await?;
    Ok(web::Json(Response { borrowed: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub borrowed: BigDecimal,
}
