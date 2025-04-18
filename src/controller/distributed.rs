use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/distributed")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state
        .database
        .tr_rewards_distribution
        .get_distributed()
        .await?;
    Ok(web::Json(Response { distributed: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub distributed: BigDecimal,
}
