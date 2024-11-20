use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::ls_loan_closing::{get_change, get_fees, get_pnl},
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

        let ((downpayment_price,), (lpn_price,), fee, (mut pnl, loan)) =
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
                get_pnl(
                    &state,
                    &lease,
                    protocol.to_owned(),
                    protocol_data.to_owned(),
                    lease.LS_contract_id.to_owned(),
                )
            )
            .context(format!(
                "could not parse currencies in lease {}",
                &lease.LS_contract_id
            ))?;

        let at = Utc::now();

        pnl += get_change(
            &state,
            sb.to_owned(),
            loan.to_string(),
            protocol.to_owned(),
            protocol_data.2.to_owned(),
            lease.LS_timestamp,
            at.to_owned(),
        )
        .await?;

        return Ok(web::Json(Some(ResponseData {
            lease,
            downpayment_price,
            lpn_price,
            fee,
            pnl: pnl.round(0),
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
    pub pnl: BigDecimal,
}
