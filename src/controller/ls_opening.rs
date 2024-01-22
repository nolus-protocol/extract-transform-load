use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Opening,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/ls-opening")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let result = state.database.ls_opening.get(data.lease.to_owned()).await?;

    if let Some(lease) = result {
        let (downpayment_price,) = state
            .database
            .mp_asset
            .get_price_by_date(&lease.LS_asset_symbol, &lease.LS_timestamp)
            .await?;
        return Ok(web::Json(Some(ResponseData { lease, downpayment_price })));
    }

    Ok(web::Json(None))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    lease: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal
}