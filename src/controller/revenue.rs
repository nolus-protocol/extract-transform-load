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
    let data = state.database.tr_profit.get_revenue().await?;
    Ok(web::Json(Response { revenue: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub revenue: BigDecimal,
}
