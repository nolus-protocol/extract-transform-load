use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

const CACHE_KEY: &str = "realized_pnl_stats";

#[get("/realized-pnl-stats")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.realized_pnl_stats, CACHE_KEY, || async {
        let data = state.database.ls_loan_closing.get_realized_pnl_stats().await?
            + BigDecimal::from_str("2958250")?;
        Ok(data.with_scale(2))
    })
    .await?;

    Ok(web::Json(ResponseData { amount: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub amount: BigDecimal,
}
