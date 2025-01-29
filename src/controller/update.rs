use std::{
    collections::BTreeMap, convert::identity, fs::File, io::BufReader,
    sync::LazyLock,
};

use actix_web::{get, web, Responder};
use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One as _};
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::{
    configuration::{AppState, State},
    error::Error,
    futures_set::{map_infallible, try_join_all},
    model::LS_Opening,
};

//TODO: delete
#[get("/update/v3/ls_loan_amnt")]
async fn ls_loan_amnt(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    static AFTER_TAXES: LazyLock<BTreeMap<&'static str, BigDecimal>> =
        LazyLock::new(|| {
            /// Represents pairs of the form
            /// `(Denominator, Mantissa * 10^(-Exponent))`, stored as
            /// `(Denominator, (Mantissa, Exponent))`.
            const AFTER_TAXES: [(&str, (u16, i8)); 34] = [
                ("ATOM", (/* 1 - 0.0076 = 0.9924 */ 9924, 4)),
                ("OSMO", (/* 1 - 0.0101 = 0.9899 */ 9899, 4)),
                ("ST_OSMO", (/* 1 - 0.0132 = 0.9868 */ 9868, 4)),
                ("ST_ATOM", (/* 1 - 0.0101 = 0.9899 */ 9899, 4)),
                ("WETH", (/* 1 - 0.0113 = 0.9887 */ 9887, 4)),
                ("WBTC", (/* 1 - 0.0103 = 0.9897 */ 9897, 4)),
                ("AKT", (/* 1 - 0.0126 = 0.9874 */ 9874, 4)),
                ("JUNO", (/* 1 - 0.0076 = 0.9924 */ 9924, 4)),
                ("AXL", (/* 1 - 0.0107 = 0.9893 */ 9893, 4)),
                ("EVMOS", (/* 1 - 0.0097 = 0.9903 */ 9903, 4)),
                ("STK_ATOM", (/* 1 - 0.0131 = 0.9869 */ 9869, 4)),
                ("SCRT", (/* 1 - 0.0074 = 0.9926 */ 9926, 4)),
                ("CRO", (/* 1 - 0.0108 = 0.9892 */ 9892, 4)),
                ("TIA", (/* 1 - 0.0089 = 0.9911 */ 9911, 4)),
                ("STARS", (/* 1 - 0.0162 = 0.9838 */ 9838, 4)),
                ("Q_ATOM", (/* 1 - 0.0113 = 0.9887 */ 9887, 4)),
                ("NTRN", (/* 1 - 0.0101 = 0.9899 */ 9899, 4)),
                ("DYDX", (/* 1 - 0.01 = 0.99 */ 99, 2)),
                ("INJ", (/* 1 - 0.0052 = 0.9948 */ 9948, 4)),
                ("STRD", (/* 1 - 0.0108 = 0.9892 */ 9892, 4)),
                ("MILK_TIA", (/* 1 - 0.0108 = 0.9892 */ 9892, 4)),
                ("ST_TIA", (/* 1 - 0.0108 = 0.9892 */ 9892, 4)),
                ("DYM", (/* 1 - 0.0041 = 0.9959 */ 9959, 4)),
                ("JKL", (/* 1 - 0.0108 = 0.9892 */ 9892, 4)),
                ("LVN", (/* 1 - 0.0067 = 0.9933 */ 9933, 4)),
                ("PICA", (/* 1 - 0.0102 = 0.9898 */ 9898, 4)),
                ("CUDOS", (/* 1 - 0.0101 = 0.9899 */ 9899, 4)),
                ("USDC", (/* 1 - 0.0098 = 0.9902 */ 9902, 4)),
                ("USDC_NOBLE", (/* 1 - 0.0098 = 0.9902 */ 9902, 4)),
                ("USDC_AXELAR", (/* 1 - 0.0098 = 0.9902 */ 9902, 4)),
                ("D_ATOM", (/* 1 - 0.0097 = 0.9903 */ 9903, 4)),
                ("QSR", (/* 1 - 0.0097 = 0.9903 */ 9903, 4)),
                ("ALL_SOL", (/* 1 - 0.0097 = 0.9903 */ 9903, 4)),
                ("ALL_BTC", (/* 1 - 0.0097 = 0.9903 */ 9903, 4)),
            ];

            AFTER_TAXES
                .into_iter()
                .map(|(denominator, (mantissa, exponent))| {
                    (
                        denominator,
                        BigDecimal::new(mantissa.into(), exponent.into()),
                    )
                })
                .collect()
        });

    try_join_all(
        state
            .database
            .ls_opening
            .get_leases_data(read_leases().await?)
            .await?
            .into_iter()
            .map(|lease| {
                ls_loan_amnt_proceed((**state).clone(), lease, &AFTER_TAXES)
            }),
        From::from,
        identity,
        (),
        |(), ()| const { Ok(()) },
        map_infallible,
        Some(state.config.max_tasks),
    )
    .await
    .map(|()| web::Json(Response { result: true }))
}

async fn ls_loan_amnt_proceed(
    state: AppState<State>,
    mut lease: LS_Opening,
    taxes: &BTreeMap<&str, BigDecimal>,
) -> Result<(), Error> {
    let lpn_currency = state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let ctrl_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .with_context(|| {
            format!("currency not found {}", lease.LS_cltr_symbol)
        })?;

    let lease_currency = state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol)
        .with_context(|| {
            format!("currency not found {}", lease.LS_cltr_symbol)
        })?;

    let loan = (&lease.LS_cltr_amnt_stable
        * BigDecimal::new(BigInt::one(), ctrl_currency.1.into()))
        + (&lease.LS_loan_amnt_stable
            * BigDecimal::new(BigInt::one(), lpn_currency.1.into()));

    let after_taxes = taxes.get(lease_currency.0.as_str()).context(format!(
        "could not get &lease_currency.0 {}",
        lease_currency.0
    ))?;

    let loan_after_taxes = (loan * after_taxes).round(0);

    let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);

    let (price,) = state
        .database
        .mp_asset
        .get_price_by_date(&lease_currency.0, protocol, &lease.LS_timestamp)
        .await?;

    let total = (loan_after_taxes
        / (price * BigDecimal::new(BigInt::one(), lease_currency.1.into())))
    .round(0);

    lease.LS_loan_amnt = total;

    state
        .database
        .ls_opening
        .update_ls_loan_amnt(&lease)
        .await
        .map_err(From::from)
}

#[get("/update/v3/ls_lpn_loan_amnt")]
async fn ls_lpn_loan_amnt(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    try_join_all(
        state
            .database
            .ls_opening
            .get_leases_data(read_leases().await?)
            .await?
            .into_iter()
            .map(|lease| ls_lpn_loan_amnt_proceed((**state).clone(), lease)),
        From::from,
        identity,
        (),
        |(), ()| const { Ok(()) },
        map_infallible,
        Some(state.config.max_tasks),
    )
    .await
    .map(|()| web::Json(Response { result: true }))
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
        .context(format!("currency not found {}", lease.LS_cltr_symbol))?;

    let lpn_price = state.database.mp_asset.get_price_by_date(
        &lpn_currency.0,
        protocol.to_owned(),
        &lease.LS_timestamp,
    );

    let lease_currency_price = state.database.mp_asset.get_price_by_date(
        &lease_currency.0,
        protocol.to_owned(),
        &lease.LS_timestamp,
    );

    let ((lpn_price,), (lease_currency_price,)) =
        tokio::try_join!(lpn_price, lease_currency_price)?;

    lease.LS_lpn_loan_amnt =
        (&lease.LS_loan_amnt * lease_currency_price) / lpn_price;

    state
        .database
        .ls_opening
        .update_ls_lpn_loan_amnt(&lease)
        .await
        .map_err(From::from)
}

async fn read_leases() -> Result<Vec<String>, Error> {
    task::spawn_blocking(|| {
        File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/leases.json"))
            .map(BufReader::new)
            .and_then(|file| serde_json::from_reader(file).map_err(From::from))
            .map_err(From::from)
    })
    .await
    .map_err(From::from)
    .and_then(identity)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}
