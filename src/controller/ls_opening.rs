use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One as _, Zero as _};
use futures::TryFutureExt as _;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::State,
    error::Error,
    handler::ls_loan_closing::get_fees,
    model::{LS_History, LS_Opening},
};

#[get("/ls-opening")]
async fn index(
    state: Data<State>,
    Query(Arguments { mut lease }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    lease.make_ascii_lowercase();

    let Some(lease) = state.database.ls_opening.get(&lease).await? else {
        return const { Ok(Json(None)) };
    };

    let protocol = state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let base_currency = state
        .config
        .hash_map_pool_currency
        .get(&lease.LS_loan_pool_id)
        .context(format!(
            "currency not found in hash map pool in protocol {}",
            &protocol
        ))?;

    let repayments_fn = state
        .database
        .ls_repayment
        .get_by_contract(&lease.LS_contract_id);

    let (downpayment_price, lpn_price, fee, repayments, history) =
        tokio::try_join!(
            state
                .database
                .mp_asset
                .get_price_by_date(
                    &lease.LS_asset_symbol,
                    Some(protocol),
                    lease.LS_timestamp,
                )
                .map_err(Error::from),
            state
                .database
                .mp_asset
                .get_price_by_date(
                    &base_currency.denominator,
                    Some(protocol),
                    lease.LS_timestamp,
                )
                .map_err(Error::from),
            get_fees(&state, &lease, protocol).map_err(Error::from),
            repayments_fn.map_err(Error::from),
            state
                .database
                .ls_opening
                .get_lease_history(&lease.LS_contract_id)
                .map_err(Error::from),
        )
        .with_context(|| {
            format!(
                "could not parse currencies in lease {}",
                lease.LS_contract_id
            )
        })?;

    repayments
        .into_iter()
        .try_fold(BigDecimal::zero(), |repayment_value, repayment| {
            state
                .config
                .hash_map_currencies
                .get(&repayment.LS_payment_symbol)
                .with_context(|| {
                    format!(
                        "currency not found {}",
                        repayment.LS_payment_symbol
                    )
                })
                .map(|currency| {
                    repayment_value
                        + (repayment.LS_payment_amnt_stable
                            * BigDecimal::new(
                                BigInt::one(),
                                currency.exponent.into(),
                            ))
                })
        })
        .map(|repayment_value| {
            Json(Some(ResponseData {
                lease,
                downpayment_price,
                lpn_price,
                fee,
                repayment_value,
                history,
            }))
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    lease: String,
}

#[derive(Debug, Serialize)]
pub struct ResponseData {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
    pub lpn_price: BigDecimal,
    pub fee: BigDecimal,
    pub repayment_value: BigDecimal,
    pub history: Vec<LS_History>,
}
