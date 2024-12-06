use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::ls_loan_closing::get_fees,
    model::{LS_History, LS_Opening},
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use futures::TryFutureExt;
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
        let repayments_fn = state
            .database
            .ls_repayment
            .get_by_contract(lease.LS_contract_id.to_owned());

        let ((downpayment_price,), (lpn_price,), fee, repayments, history) =
            tokio::try_join!(
                state
                    .database
                    .mp_asset
                    .get_price_by_date(
                        &lease.LS_asset_symbol,
                        Some(protocol.to_owned()),
                        &lease.LS_timestamp,
                    )
                    .map_err(Error::from),
                state
                    .database
                    .mp_asset
                    .get_price_by_date(
                        base_currency,
                        Some(protocol.to_owned()),
                        &lease.LS_timestamp,
                    )
                    .map_err(Error::from),
                get_fees(&state, &lease, protocol.to_owned())
                    .map_err(Error::from),
                repayments_fn.map_err(Error::from),
                state
                    .database
                    .ls_opening
                    .get_lease_history(lease.LS_contract_id.to_owned())
                    .map_err(Error::from),
            )
            .context(format!(
                "could not parse currencies in lease {}",
                &lease.LS_contract_id
            ))?;

        let mut repayment_value = BigDecimal::from(0);

        for repayment in repayments {
            let currency = state
                .config
                .hash_map_currencies
                .get(&repayment.LS_payment_symbol)
                .context(format!(
                    "currency not found  {}",
                    &repayment.LS_payment_symbol
                ))?;
            repayment_value += repayment.LS_payment_amnt_stable
                / BigDecimal::from(u64::pow(10, currency.1.try_into()?));
        }

        return Ok(web::Json(Some(ResponseData {
            lease,
            downpayment_price,
            lpn_price,
            fee,
            repayment_value,
            history,
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
    pub fee: BigDecimal,
    pub repayment_value: BigDecimal,
    pub history: Vec<LS_History>,
}
