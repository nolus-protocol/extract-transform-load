use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/realized-pnl-stats")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state
        .database
        .ls_loan_closing
        .get_realized_pnl_stats()
        .await?
        + BigDecimal::from_str("2958250")?;

    Ok(web::Json(ResponseData {
        amount: data.with_scale(2),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub amount: BigDecimal,
}
