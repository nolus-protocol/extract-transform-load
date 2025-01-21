use std::{
    collections::BTreeMap,
    fs::File,
    future::Future,
    io::BufReader,
    sync::{Arc, LazyLock},
};

use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};
use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One};
use serde::Serialize;

use crate::{
    configuration::State, error::Error, model::LS_Opening,
    try_join_with_capacity,
};

/// Represents pairs of the form `(Denominator, Mantissa * 10^(-Exponent))`,
/// stored as `(Denominator, (Mantissa, Exponent))`.
const TAXES_LOOKUP_TABLE: [(&'static str, (u8, u8)); 34] = [
    ("ATOM", /* 0.0076 */ (76, 4)),
    ("OSMO", /* 0.0101 */ (101, 4)),
    ("ST_OSMO", /* 0.0132 */ (132, 4)),
    ("ST_ATOM", /* 0.0101 */ (101, 4)),
    ("WETH", /* 0.0113 */ (113, 4)),
    ("WBTC", /* 0.0103 */ (103, 4)),
    ("AKT", /* 0.0126 */ (126, 4)),
    ("JUNO", /* 0.0076 */ (76, 4)),
    ("AXL", /* 0.0107 */ (107, 4)),
    ("EVMOS", /* 0.0097 */ (97, 4)),
    ("STK_ATOM", /* 0.0131 */ (131, 4)),
    ("SCRT", /* 0.0074 */ (74, 4)),
    ("CRO", /* 0.0108 */ (108, 4)),
    ("TIA", /* 0.0089 */ (89, 4)),
    ("STARS", /* 0.0162 */ (162, 4)),
    ("Q_ATOM", /* 0.0113 */ (113, 4)),
    ("NTRN", /* 0.0101 */ (101, 4)),
    ("DYDX", /* 0.0100 */ (100, 4)),
    ("INJ", /* 0.0052 */ (52, 4)),
    ("STRD", /* 0.0108 */ (108, 4)),
    ("MILK_TIA", /* 0.0108 */ (108, 4)),
    ("ST_TIA", /* 0.0108 */ (108, 4)),
    ("DYM", /* 0.0041 */ (41, 4)),
    ("JKL", /* 0.0108 */ (108, 4)),
    ("LVN", /* 0.0067 */ (67, 4)),
    ("PICA", /* 0.0102 */ (102, 4)),
    ("CUDOS", /* 0.0101 */ (101, 4)),
    ("USDC", /* 0.0098 */ (98, 4)),
    ("USDC_NOBLE", /* 0.0098 */ (98, 4)),
    ("USDC_AXELAR", /* 0.0098 */ (98, 4)),
    ("D_ATOM", /* 0.0097 */ (97, 4)),
    ("QSR", /* 0.0097 */ (97, 4)),
    ("ALL_SOL", /* 0.0097 */ (97, 4)),
    ("ALL_BTC", /* 0.0097 */ (97, 4)),
];

static TAXES: LazyLock<BTreeMap<&str, BigDecimal>> = LazyLock::new(|| {
    TAXES_LOOKUP_TABLE
        .map(|(denominator, (mantissa, exponent))| {
            (
                denominator,
                BigDecimal::new(mantissa.into(), exponent.into()),
            )
        })
        .into()
});

//TODO: delete
#[get("/update/v3/ls_loan_amnt")]
async fn ls_loan_amnt(state: Data<State>) -> Result<impl Responder, Error> {
    ls_loan_amnt_task(&state)
        .await
        .map(|()| const { Json(Response { result: true }) })
}

async fn ls_loan_amnt_task(state: &Arc<State>) -> Result<(), Error> {
    let ls: Vec<String> =
        File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/leases.json"))
            .map_err(Error::from)
            .and_then(|file| {
                serde_json::from_reader(BufReader::new(file))
                    .map_err(Error::from)
            })?;

    try_join_with_capacity(
        state
            .database
            .ls_opening
            .get_leases_data(&ls)
            .await?
            .into_iter()
            .map(|lease| ls_loan_amnt_proceed(state.clone(), lease, &TAXES))
            .fuse(),
        state.config.max_tasks,
    )
    .await
}

async fn ls_loan_amnt_proceed(
    state: Arc<State>,
    mut lease: LS_Opening,
    hash: &BTreeMap<&str, BigDecimal>,
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

    let mut loan = (&lease.LS_cltr_amnt_stable
        * BigDecimal::new(BigInt::one(), ctrl_currency.exponent.into()))
        + (&lease.LS_loan_amnt_stable
            / BigDecimal::new(BigInt::one(), lpn_currency.exponent.into()));

    let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);

    let price = state
        .database
        .mp_asset
        .get_price_by_date(
            &lease_currency.denominator,
            protocol,
            lease.LS_timestamp,
        )
        .await?;

    let taxes = hash.get(&lease_currency.denominator).context(format!(
        "could not get &lease_currency.0 {}",
        lease_currency.denominator
    ))?;

    loan *= BigDecimal::one() - taxes;

    loan = loan.round(0);

    let currency_exponent =
        BigDecimal::new(BigInt::one(), lease_currency.exponent.into());

    state
        .database
        .ls_opening
        .update_ls_loan_amount(
            &lease.LS_contract_id,
            &(loan / (price * currency_exponent)).round(0),
        )
        .await
        .map_err(From::from)
}

#[get("/update/v3/ls_lpn_loan_amnt")]
async fn ls_lpn_loan_amnt(state: Data<State>) -> Result<impl Responder, Error> {
    ls_lpn_loan_amnt_task(&state)
        .await
        .map(|()| const { Json(Response { result: true }) })
}

async fn ls_lpn_loan_amnt_task(state: &Arc<State>) -> Result<(), Error> {
    let ls: Vec<String> =
        File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/leases.json"))
            .map_err(From::from)
            .and_then(serde_json::from_reader)
            .map_err(From::from)?;

    state
        .database
        .ls_opening
        .get_leases_data(ls)
        .await
        .map_err(From::from)
        .and_then(|leases| {
            try_join_with_capacity(
                leases.into_iter().map(|lease| {
                    ls_lpn_loan_amnt_proceed(state.clone(), lease)
                }),
                state.config.max_tasks,
            )
        })
}

async fn ls_lpn_loan_amnt_proceed(
    state: Arc<State>,
    mut lease: LS_Opening,
) -> Result<(), Error> {
    let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);
    let lpn_currency = state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let lease_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol)
        .context(format!("currency not found {}", &lease.LS_cltr_symbol))?;

    let lpn_price = state.database.mp_asset.get_price_by_date(
        &lpn_currency.denominator,
        protocol,
        lease.LS_timestamp,
    );

    let lease_currency_price = state.database.mp_asset.get_price_by_date(
        &lease_currency.denominator,
        protocol,
        lease.LS_timestamp,
    );

    let (lpn_price, lease_currency_price) =
        tokio::try_join!(lpn_price, lease_currency_price)?;

    state
        .database
        .ls_opening
        .update_ls_lpn_loan_amount(
            &lease.LS_contract_id,
            &((&lease.LS_loan_amnt * lease_currency_price) / lpn_price),
        )
        .await
        .map_err(From::from)
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub result: bool,
}
