use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Opening,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

#[get("/ls-openings")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let leases: Vec<&str> = data.leases.split(',').collect();
    let data = state.database.ls_opening.get_leases(leases).await?;
    let mut joins = Vec::new();

    for item in data {
        joins.push(getData(state.clone(), item))
    }

    let result = join_all(joins).await;
    let mut items: Vec<ResponseData> = vec![];

    for item in result.into_iter().flatten().flatten() {
        items.push(item);
    }

    Ok(web::Json(items))
}

async fn getData(
    state: web::Data<AppState<State>>,
    lease: LS_Opening,
) -> Result<Option<ResponseData>, Error> {
    let result = state
        .database
        .ls_opening
        .get(lease.LS_contract_id.to_owned())
        .await?;
    if let Some(lease) = result {
        let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);
        let (downpayment_price,) = state
            .database
            .mp_asset
            .get_price_by_date(
                &lease.LS_asset_symbol,
                protocol,
                &lease.LS_timestamp,
            )
            .await?;
        return Ok(Some(ResponseData {
            lease,
            downpayment_price,
        }));
    }
    Ok(None)
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
