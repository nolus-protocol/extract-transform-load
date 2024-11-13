use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::ls_loan_closing::get_fees,
    helpers::Protocol_Types,
    model::LS_Opening,
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::Utc;
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
        let protocol_data = state
            .config
            .hash_map_lp_pools
            .get(&lease.LS_loan_pool_id)
            .context("could not load protocol")?;

        let base_currency = state
            .config
            .hash_map_pool_currency
            .get(&lease.LS_loan_pool_id)
            .context(format!(
                "currency not found in hash map pool in protocol {}",
                &protocol
            ))?;

        let base_currency = &base_currency.0;
        let symbol = lease.LS_asset_symbol.to_owned();

        let sb = match protocol_data.2 {
            Protocol_Types::Long => symbol,
            Protocol_Types::Short => protocol_data.1.to_owned(),
        };

        let ((downpayment_price,), (lpn_price,), fee, amount) =
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
                get_fees(&state, &lease, Some(protocol.to_owned()))
                    .map_err(Error::from),
                state
                    .database
                    .ls_loan_closing
                    .get_lease_amount(lease.LS_contract_id.to_owned())
                    .map_err(Error::from)
            )
            .context(format!(
                "could not parse currencies in lease {}",
                &lease.LS_contract_id
            ))?;

        let loan_str = &amount.to_string();

        let f1 = state.in_stabe_by_date(
            &sb,
            loan_str,
            Some(protocol.to_owned()),
            &lease.LS_timestamp,
        );

        let at = Utc::now();

        let f2 = state.in_stabe_by_date(
            &sb,
            loan_str,
            Some(protocol.to_owned()),
            &at,
        );

        let (open_amount, close_amount) = tokio::try_join!(f1, f2,)?;

        return Ok(web::Json(Some(ResponseData {
            lease,
            downpayment_price,
            lpn_price,
            fee,
            amount,
            pnl: (close_amount - open_amount).round(0),
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
    pub amount: BigDecimal,
    pub pnl: BigDecimal,
}
