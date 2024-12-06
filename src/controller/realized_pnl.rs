use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

#[get("/realized-pnl")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_loan_closing
        .get_realized_pnl(address)
        .await?;
    let mut pnl = BigDecimal::from(0);

    for item in data {
        let protocol_data = state
            .config
            .hash_map_lp_pools
            .get(&item.LS_loan_pool_id)
            .context(format!(
                "could not get protocol in realized-pnl{}",
                &item.LS_loan_pool_id
            ))?;
        let currency = match protocol_data.2 {
            crate::helpers::Protocol_Types::Long => state
                .config
                .hash_map_currencies
                .get(&item.LS_asset_symbol)
                .context(format!(
                    "LS_asset_symbol not found {}",
                    &item.LS_asset_symbol
                ))?,
            crate::helpers::Protocol_Types::Short => {
                state.get_currency_by_pool_id(&item.LS_loan_pool_id)?
            },
        };
        let amount = BigDecimal::from(item.LS_pnl)
            / BigDecimal::from(u64::pow(10, currency.1.try_into()?));

        pnl += amount;
    }

    Ok(web::Json(Response { realized_pnl: pnl }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub realized_pnl: BigDecimal,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}
