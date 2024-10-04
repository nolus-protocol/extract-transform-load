use std::collections::HashMap;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Opening,
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

//TODO: delete
#[get("/update/v3/ls_loan_amnt")]
async fn ls_loan_amnt(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    ls_loan_amnt_task(state.as_ref()).await?;
    Ok(web::Json(Response { result: true }))
}

async fn ls_loan_amnt_task(state: &AppState<State>) -> Result<(), Error> {
    let data = vec![
        ("ATOM", 0.0076),
        ("OSMO", 0.0101),
        ("ST_OSMO", 0.0132),
        ("ST_ATOM", 0.0101),
        ("WETH", 0.0113),
        ("WBTC", 0.0103),
        ("AKT", 0.0126),
        ("JUNO", 0.0076),
        ("AXL", 0.0107),
        ("EVMOS", 0.0097),
        ("STK_ATOM", 0.0131),
        ("SCRT", 0.0074),
        ("CRO", 0.0108),
        ("TIA", 0.0089),
        ("STARS", 0.0162),
        ("Q_ATOM", 0.0113),
        ("NTRN", 0.0101),
        ("DYDX", 0.0100),
        ("INJ", 0.0052),
        ("STRD", 0.0108),
        ("MILK_TIA", 0.0108),
        ("ST_TIA", 0.0108),
        ("DYM", 0.0041),
        ("JKL", 0.0108),
        ("LVN", 0.0067),
        ("PICA", 0.0102),
        ("CUDOS", 0.0101),
        ("USDC", 0.0098),
        ("USDC_NOBLE", 0.0098),
        ("USDC_AXELAR", 0.0098),
        ("D_ATOM", 0.0097),
        ("QSR", 0.0097),
        ("ALL_SOL", 0.0097),
        ("ALL_BTC", 0.0097),
    ];

    let mut hash = HashMap::new();

    for c in data {
        hash.insert(c.0, c);
    }

    let leases = state.database.ls_opening.get_leases_data().await?;
    let mut tasks = vec![];
    let max_tasks = state.config.max_tasks;

    for lease in leases {
        tasks.push(ls_loan_amnt_proceed(state.clone(), lease, hash.clone()));
    }

    while !tasks.is_empty() {
        let mut st = JoinSet::new();
        let range = if tasks.len() > max_tasks {
            max_tasks
        } else {
            tasks.len()
        };

        for _t in 0..range {
            if let Some(item) = tasks.pop() {
                st.spawn(item);
            }
        }

        while let Some(item) = st.join_next().await {
            item??;
        }
    }

    Ok(())
}

async fn ls_loan_amnt_proceed(
    state: AppState<State>,
    mut lease: LS_Opening,
    hash: HashMap<&str, (&str, f64)>,
) -> Result<(), Error> {
    let lpn_currency = state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let ctrl_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!("currency not found {}", &lease.LS_cltr_symbol))?;

    let lease_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol)
        .context(format!("currency not found {}", &lease.LS_cltr_symbol))?;

    let loan = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, ctrl_currency.1.try_into()?))
        + &lease.LS_loan_amnt_stable
            / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));

    let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);

    let (price,) = state
        .database
        .mp_asset
        .get_price_by_date(
            &lease_currency.0,
            protocol.to_owned(),
            &lease.LS_timestamp,
        )
        .await?;
    let taxes = hash.get(lease_currency.0.as_str()).context(format!(
        "could not get &lease_currency.0 {}",
        &lease_currency.0
    ))?;
    let loan = (&loan - &loan * BigDecimal::try_from(taxes.1)?).round(0);
    let total = (loan / price
        * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)))
    .round(0);

    let total = (total).round(0);
    lease.LS_loan_amnt = total;
    state
        .database
        .ls_opening
        .update_ls_loan_amnt(&lease)
        .await?;

    Ok(())
}

#[get("/update/v3/ls_lpn_loan_amnt")]
async fn ls_lpn_loan_amnt(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    ls_lpn_loan_amnt_task(state.as_ref().clone()).await?;
    Ok(web::Json(Response { result: true }))
}

async fn ls_lpn_loan_amnt_task(state: AppState<State>) -> Result<(), Error> {
    let leases = state.database.ls_opening.get_leases_data_lpn_amnt().await?;

    let mut tasks = vec![];
    let max_tasks = state.config.max_tasks;

    for lease in leases {
        tasks.push(ls_lpn_loan_amnt_proceed(state.clone(), lease));
    }

    while !tasks.is_empty() {
        let mut st = JoinSet::new();
        let range = if tasks.len() > max_tasks {
            max_tasks
        } else {
            tasks.len()
        };

        for _t in 0..range {
            if let Some(item) = tasks.pop() {
                st.spawn(item);
            }
        }

        while let Some(item) = st.join_next().await {
            item??;
        }
    }
    Ok(())
}

async fn ls_lpn_loan_amnt_proceed(
    state: AppState<State>,
    mut lease: LS_Opening,
) -> Result<(), Error> {
    let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);
    let lpn_currency = state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let lease_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol)
        .context(format!("currency not found {}", &lease.LS_cltr_symbol))?;

    let f1 = state.database.mp_asset.get_price_by_date(
        &lpn_currency.0,
        protocol.to_owned(),
        &lease.LS_timestamp,
    );

    let f2 = state.database.mp_asset.get_price_by_date(
        &lease_currency.0,
        protocol.to_owned(),
        &lease.LS_timestamp,
    );

    let (lpn_price, lease_currency_price) = tokio::try_join!(f1, f2)?;
    let (lpn_price,) = lpn_price;
    let (lease_currency_price,) = lease_currency_price;

    lease.LS_lpn_loan_amnt =
        &lease.LS_loan_amnt * lease_currency_price / lpn_price;

    state
        .database
        .ls_opening
        .update_ls_lpn_loan_amnt(&lease)
        .await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}
