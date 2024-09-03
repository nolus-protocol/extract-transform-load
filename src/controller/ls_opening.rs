use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Opening,
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/ls-opening")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let result = state.database.ls_opening.get(data.lease.to_owned()).await?;
    if let Some(lease) = result {
        let protocol = state
            .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
            .context(format!(
                "protocol not found {}",
                &lease.LS_loan_pool_id
            ))?;

        let base_currency = state
            .config
            .hash_map_pool_currency
            .get(&lease.LS_loan_pool_id)
            .context(format!(
                "currency not found in hash map pool in protocol {}",
                &protocol
            ))?;

        let base_currency = &base_currency.0;

        let ((downpayment_price,), (lpn_price,)) = tokio::try_join!(
            state.database.mp_asset.get_price_by_date(
                &lease.LS_asset_symbol,
                Some(protocol.to_owned()),
                &lease.LS_timestamp,
            ),
            state.database.mp_asset.get_price_by_date(
                base_currency,
                Some(protocol.to_owned()),
                &lease.LS_timestamp,
            )
        )
        .context(format!(
            "could not parse currencies in lease {}",
            &lease.LS_contract_id
        ))?;

        return Ok(web::Json(Some(ResponseData {
            lease,
            downpayment_price,
            lpn_price,
        })));
    }

    Ok(web::Json(None))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    lease: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
    pub lpn_price: BigDecimal,
}
