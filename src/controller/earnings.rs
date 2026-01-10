use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/earnings")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let earnings = state.database.lp_pool_state.get_earnings(address).await?;
    Ok(web::Json(Response { earnings }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub earnings: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
