use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One as _, Zero as _};
use serde::{Deserialize, Serialize};

use crate::{configuration::State, error::Error, helpers::Protocol_Types};

#[get("/realized-pnl")]
async fn index(
    state: Data<State>,
    Query(Arguments { mut address }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    address.make_ascii_lowercase();

    state
        .database
        .ls_loan_closing
        .get_realized_pnl(&address)
        .await?
        .into_iter()
        .try_fold(BigDecimal::zero(), |acc, item| {
            let (_, _, protocol_data) = state
                .config
                .hash_map_lp_pools
                .get(&item.LS_loan_pool_id)
                .context(format!(
                    "could not get protocol in realized-pnl{}",
                    item.LS_loan_pool_id
                ))?;

            let currency = match protocol_data {
                Protocol_Types::Long => state
                    .config
                    .hash_map_currencies
                    .get(&item.LS_asset_symbol)
                    .context(format!(
                        "LS_asset_symbol not found {}",
                        item.LS_asset_symbol
                    ))?,
                Protocol_Types::Short => {
                    state.get_currency_by_pool_id(&item.LS_loan_pool_id)?
                },
            };

            Ok(acc
                + (item.LS_pnl
                    * BigDecimal::new(BigInt::one(), currency.exponent.into())))
        })
        .map(|realized_pnl| Json(Response { realized_pnl }))
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    address: String,
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub realized_pnl: BigDecimal,
}
