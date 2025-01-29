use std::convert::identity;

use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    futures_set::{map_infallible, try_join_all},
    model::LS_Opening,
};

#[get("/ls-openings")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    try_join_all(
        state
            .database
            .ls_opening
            .get_leases(data.leases.split(',').collect())
            .await?
            .into_iter()
            .map(|ls_opening| getData(state.clone(), ls_opening)),
        From::from,
        identity,
        vec![],
        |mut accumulator, response_data| {
            accumulator.push(response_data);

            Ok(accumulator)
        },
        map_infallible,
        None,
    )
    .await
    .map(web::Json)
}

async fn getData(
    state: web::Data<AppState<State>>,
    lease: LS_Opening,
) -> Result<ResponseData, Error> {
    state
        .database
        .mp_asset
        .get_price_by_date(
            &lease.LS_asset_symbol,
            state.get_protocol_by_pool_id(&lease.LS_loan_pool_id),
            &lease.LS_timestamp,
        )
        .await
        .map(|(downpayment_price,)| ResponseData {
            lease,
            downpayment_price,
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Query {
    pub leases: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
}
