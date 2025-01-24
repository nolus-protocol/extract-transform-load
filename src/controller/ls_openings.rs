use std::iter;

use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    futures_set::try_join_all_folding_with_capacity,
    model::LS_Opening,
};

#[get("/ls-openings")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let leases_count;

    let leases = {
        let leases = data.leases.split(',').collect::<Vec<_>>();

        leases_count = leases.len();

        state.database.ls_opening.get_leases(leases).await?
    };

    let results_vec: Vec<_> = iter::from_fn(|| const { Some(None) })
        .take(leases_count - leases.len())
        .collect();

    try_join_all_folding_with_capacity(
        leases
            .into_iter()
            .map(|ls_opening| getData(state.clone(), ls_opening)),
        results_vec,
        |mut accumulator, response_data| {
            accumulator.push(Some(response_data));

            accumulator
        },
        state.config.max_tasks,
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
