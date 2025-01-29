use std::{collections::HashMap, convert::identity, str::FromStr as _};

use anyhow::Context as _;
use chrono::Utc;
use sqlx::types::BigDecimal;
use tokio::{time, time::Duration};
use tracing::error;

use crate::{
    configuration::{AppState, State},
    error::Error,
    futures_set::{map_infallible, try_join_all},
    model::{Action_History, Actions, MP_Asset},
};

pub async fn fetch_insert(
    app_state: AppState<State>,
    height: Option<String>,
) -> Result<(), Error> {
    let timestamp = Utc::now();

    let (mut mp_assets, lpns) = try_join_all(
        protocol_names_iter(&app_state)
            .map(|protocol| get_lpn_data(app_state.clone(), protocol)),
        From::from,
        identity,
        (Vec::new(), HashMap::new()),
        |(mut mp_assets, mut lpns),
         (protocol, base_currency, price, decimal)| {
            mp_assets.push(MP_Asset {
                MP_asset_symbol: base_currency.to_owned(),
                MP_asset_timestamp: timestamp,
                MP_price_in_stable: price.to_owned(),
                Protocol: protocol.to_owned(),
            });

            lpns.insert(protocol, (base_currency, price, decimal));

            Ok((mp_assets, lpns))
        },
        map_infallible,
        None,
    )
    .await?;

    mp_assets = try_join_all(
        protocol_oracle_and_names_iter(&app_state).map({
            |ProtocolNameAndOracle { protocol, oracle }| {
                let app_state = app_state.clone();

                let height = height.clone();

                async move {
                    app_state.grpc.get_prices(oracle, protocol, height).await
                }
            }
        }),
        From::from,
        From::from,
        mp_assets,
        |mut mp_assets, data| {
            let (assets, protocol) = data;
            let (_base_currency, lpn_price, lpn_decimals) = lpns
                .get(&protocol)
                .context(format!("lpn not found in protocol {}", &protocol))?;

            for price in assets.prices {
                if let Some(asset) = app_state
                    .config
                    .hash_map_currencies
                    .get(&price.amount.ticker)
                {
                    let decimals = asset.1 - lpn_decimals;
                    let mut value =
                        BigDecimal::from_str(&price.amount_quote.amount)?
                            / BigDecimal::from_str(&price.amount.amount)?
                            * lpn_price;
                    let decimals_abs = decimals.abs();

                    let power_value = BigDecimal::from(u64::pow(
                        10,
                        decimals_abs.try_into()?,
                    ));

                    if decimals > 0 {
                        value *= power_value;
                    } else {
                        value = value / power_value;
                    }

                    let mp_asset = MP_Asset {
                        MP_asset_symbol: price.amount.ticker.to_owned(),
                        MP_asset_timestamp: timestamp,
                        MP_price_in_stable: value,
                        Protocol: protocol.to_owned(),
                    };

                    mp_assets.push(mp_asset);
                }
            }

            Ok(mp_assets)
        },
        identity::<Error>,
        None,
    )
    .await?;

    app_state.database.mp_asset.insert_many(&mp_assets).await?;

    let action_history = Action_History {
        action_type: Actions::MpAssetAction.to_string(),
        created_at: timestamp,
    };

    app_state
        .database
        .action_history
        .insert(action_history)
        .await
        .map_err(From::from)
}

#[inline]
fn protocol_names_iter(
    app_state: &AppState<State>,
) -> impl Iterator<Item = String> + '_ + use<'_> {
    app_state
        .protocols
        .values()
        .map(|protocol| protocol.protocol.clone())
}

struct ProtocolNameAndOracle {
    protocol: String,
    oracle: String,
}

#[inline]
fn protocol_oracle_and_names_iter(
    app_state: &AppState<State>,
) -> impl Iterator<Item = ProtocolNameAndOracle> + '_ + use<'_> {
    app_state
        .protocols
        .values()
        .map(|protocol| ProtocolNameAndOracle {
            protocol: protocol.protocol.clone(),
            oracle: protocol.contracts.oracle.clone(),
        })
}

pub async fn mp_assets_task(app_state: AppState<State>) -> Result<(), Error> {
    let mut interval = time::interval(Duration::from_secs(
        app_state.config.mp_asset_interval.into(),
    ));

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let _ = fetch_insert(app_state.clone(), None).await.inspect_err(
                |error| {
                    error!("Task error {error}");
                },
            );
        }
    })
    .await?
}

pub async fn get_lpn_data(
    app_state: AppState<State>,
    protocol: String,
) -> Result<(String, String, BigDecimal, i16), Error> {
    let prtcs = app_state
        .protocols
        .get(&protocol)
        .context(format!("protocol not found {}", &protocol))?;

    let base_currency = app_state
        .grpc
        .get_base_currency(prtcs.contracts.oracle.to_owned())
        .await?;

    let lpn_price = app_state
        .grpc
        .get_stable_price(
            prtcs.contracts.oracle.to_owned(),
            base_currency.to_owned(),
        )
        .await?;

    let lpn_decimals = app_state
        .config
        .hash_map_currencies
        .get(&base_currency)
        .context(format!("currency not found {}", &base_currency))?
        .1;

    let asset = app_state
        .config
        .hash_map_currencies
        .get(&lpn_price.amount_quote.ticker)
        .context(format!(
            "could not find currency {}",
            &lpn_price.amount_quote.ticker,
        ))?;

    let decimals = asset.1 - lpn_decimals;
    let decimals_abs = decimals.abs();
    let power_value = BigDecimal::from(u64::pow(10, decimals_abs.try_into()?));

    let mut lpn_price = BigDecimal::from_str(&lpn_price.amount_quote.amount)?
        / BigDecimal::from_str(&lpn_price.amount.amount)?;

    if decimals > 0 {
        lpn_price = lpn_price / power_value;
    } else {
        lpn_price *= power_value;
    }

    Ok((protocol, base_currency, lpn_price, lpn_decimals))
}
