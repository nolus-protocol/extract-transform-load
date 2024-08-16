use anyhow::Context;
use chrono::Utc;
use futures::future::join_all;
use sqlx::types::BigDecimal;
use tokio::{time, time::Duration};
use tracing::error;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Actions,
    model::{Action_History, MP_Asset},
};
use std::{collections::HashMap, str::FromStr};

pub async fn fetch_insert(
    app_state: AppState<State>,
    height: Option<String>,
) -> Result<(), Error> {
    let mut joins = vec![];
    let mut protocl_data_joins = vec![];
    let mut mp_assets = vec![];
    let timestamp = Utc::now();
    let mut lpns = HashMap::new();

    for protocol in app_state.protocols.values() {
        protocl_data_joins.push(get_lpn_data(
            app_state.clone(),
            protocol.protocol.to_owned(),
        ));
        joins.push(app_state.grpc.get_prices(
            protocol.contracts.oracle.to_owned(),
            protocol.protocol.to_owned(),
            height.to_owned(),
        ));
    }

    for result in join_all(protocl_data_joins).await {
        match result {
            Ok((protocol, base_currency, price, decimal)) => {
                let mp_asset = MP_Asset {
                    MP_asset_symbol: base_currency.to_owned(),
                    MP_asset_timestamp: timestamp,
                    MP_price_in_stable: price.to_owned(),
                    Protocol: protocol.to_owned(),
                };
                mp_assets.push(mp_asset);
                lpns.insert(protocol, (base_currency, price, decimal));
            },
            Err(err) => {
                return Err(err);
            },
        }
    }

    for result in join_all(joins).await {
        match result {
            Ok(data) => {
                let (assets, protocol) = data;
                let (_base_currency, lpn_price, lpn_decimals) =
                    lpns.get(&protocol).context(format!(
                        "lpn not found in protocol {}",
                        &protocol
                    ))?;

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
            },
            Err(err) => {
                return Err(err);
            },
        }
    }

    app_state.database.mp_asset.insert_many(&mp_assets).await?;

    let action_history = Action_History {
        action_type: Actions::MpAssetAction.to_string(),
        created_at: timestamp,
    };

    app_state
        .database
        .action_history
        .insert(action_history)
        .await?;

    Ok(())
}

pub async fn mp_assets_task(app_state: AppState<State>) -> Result<(), Error> {
    let interval: u64 = app_state.config.mp_asset_interval.into();
    let interval: u64 = interval * 60;

    let mut interval = time::interval(Duration::from_secs(interval));
    tokio::spawn(async move {
        interval.tick().await;
        loop {
            interval.tick().await;
            let app = app_state.clone();
            if let Err(error) = fetch_insert(app, None).await {
                error!("Task error {}", error);
            };
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

    let lpn_price = BigDecimal::from_str(&lpn_price.amount_quote.amount)?
        / BigDecimal::from_str(&lpn_price.amount.amount)?;

    let lpn_decimals = app_state
        .config
        .hash_map_currencies
        .get(&base_currency)
        .context(format!("currency not found {}", &base_currency))?
        .1;

    Ok((protocol, base_currency, lpn_price, lpn_decimals))
}
