use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::ls_loan_closing::{get_fees, get_pnl_long, get_pnl_short},
    helpers::{Loan_Closing_Status, Protocol_Types},
    model::{LS_Loan, LS_Opening},
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
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
        let at = Utc::now();
        let app_state = &state.clone();

        let ((downpayment_price,), (lpn_price,), fee, pnl_res) =
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
                get_pnl(app_state, &lease, at)
            )
            .context(format!(
                "could not parse currencies in lease {}",
                &lease.LS_contract_id
            ))?;

        let pnl = pnl_res.LS_pnl;

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

pub async fn get_pnl(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let protocol_data = app_state
        .config
        .hash_map_lp_pools
        .get(&lease.LS_loan_pool_id)
        .context("could not load protocol")?;

    match protocol_data.2 {
        Protocol_Types::Short => {
            get_pnl_short(
                app_state,
                &lease,
                lease.LS_contract_id.to_owned(),
                (BigDecimal::from(0), Loan_Closing_Status::None),
                BigDecimal::from(0),
                at,
            )
            .await
        },
        Protocol_Types::Long => {
            get_pnl_long(
                app_state,
                &lease,
                lease.LS_contract_id.to_owned(),
                (BigDecimal::from(0), Loan_Closing_Status::None),
                BigDecimal::from(0),
                at,
            )
            .await
        },
    }
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
