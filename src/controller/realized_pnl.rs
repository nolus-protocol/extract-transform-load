use actix_web::{get, web, Responder};
use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One as _, Zero as _};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/realized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(Query { mut address }): web::Query<Query>,
) -> Result<impl Responder, Error> {
    address.make_ascii_lowercase();

    state
        .database
        .ls_loan_closing
        .get_realized_pnl(address)
        .await
        .map_err(From::from)
        .and_then(|data| {
            data.into_iter()
                .try_fold(BigDecimal::zero(), |pnl, item| {
                    state
                        .config
                        .hash_map_lp_pools
                        .get(&item.LS_loan_pool_id)
                        .context(format!(
                            "could not get protocol in realized-pnl {}",
                            item.LS_loan_pool_id
                        ))
                        .map_err(From::from)
                        .and_then(|(_, _, protocol_type)| match protocol_type {
                            crate::helpers::Protocol_Types::Long => state
                                .config
                                .hash_map_currencies
                                .get(&item.LS_asset_symbol)
                                .context(format!(
                                    "LS_asset_symbol not found {}",
                                    item.LS_asset_symbol
                                ))
                                .map_err(From::from),
                            crate::helpers::Protocol_Types::Short => state
                                .get_currency_by_pool_id(&item.LS_loan_pool_id),
                        })
                        .map(|currency| {
                            pnl + (item.LS_pnl
                                * BigDecimal::new(
                                    BigInt::one(),
                                    currency.1.into(),
                                ))
                        })
                })
                .map(|realized_pnl| web::Json(Response { realized_pnl }))
        })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub realized_pnl: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
