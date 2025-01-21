use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::State, error::Error, model::LS_Opening,
    try_join_with_capacity,
};

#[get("/ls-openings")]
async fn index(
    state: Data<State>,
    Query(Arguments { leases }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    try_join_with_capacity::<_, Vec<_>, _, _, _>(
        state
            .database
            .ls_opening
            .get_leases(leases.split(','))
            .await?
            .into_iter()
            .map(|item| getData((**state).clone(), item)),
        state.config.max_tasks,
    )
    .await
    .map(Json)
}

async fn getData(
    state: AppState<State>,
    lease: LS_Opening,
) -> Result<ResponseData, Error> {
    state
        .database
        .mp_asset
        .get_price_by_date(
            &lease.LS_asset_symbol,
            state.get_protocol_by_pool_id(&lease.LS_loan_pool_id),
            lease.LS_timestamp,
        )
        .await
        .map(|downpayment_price| ResponseData {
            lease,
            downpayment_price,
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    pub leases: String,
}

#[derive(Debug, Serialize)]
pub struct ResponseData {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
}
