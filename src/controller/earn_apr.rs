use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/earn-apr")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.ls_opening.get_earn_apr().await?;
    Ok(web::Json(Response { earn_apr: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub earn_apr: BigDecimal,
}
