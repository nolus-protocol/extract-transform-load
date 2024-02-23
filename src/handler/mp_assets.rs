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
use std::str::FromStr;

pub async fn fetch_insert(app_state: AppState<State>, height: Option<String>) -> Result<(), Error> {
    let mut joins = Vec::new();
    let mut mp_assets = vec![];
    let timestamp = Utc::now();

    for protocol in app_state.protocols.values() {
        joins.push(app_state.query_api.get_prices(
            protocol.contracts.oracle.to_owned(),
            protocol.network.to_owned(),
            height.to_owned(),
        ));
    }
    for result in join_all(joins).await {
        match result {
            Ok(data) => {
                let (assets, protocol) = data;
                if let Some(item) = assets {

                    for price in item.prices {
                        if let Some(asset) = app_state
                            .config
                            .hash_map_currencies
                            .get(&price.amount.ticker)
                        {
                            let decimals = asset.2 - app_state.config.lpn_decimals;
                            let mut value = BigDecimal::from_str(&price.amount_quote.amount)?
                                / BigDecimal::from_str(&price.amount.amount)?;
                            let decimals_abs = decimals.abs();

                            let power_value =
                                BigDecimal::from(u64::pow(10, decimals_abs.try_into()?));

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
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    for (protocol, config) in &app_state.protocols {
        let item = app_state
            .config
            .lp_pools
            .iter()
            .find(|(contract, _currency)| contract == &config.contracts.lpp);

        match item {
            Some((_contract, currency)) => {
                let value = app_state.config.lpn_price.to_owned();
                let mp_asset = MP_Asset {
                    MP_asset_symbol: currency.to_owned(),
                    MP_asset_timestamp: timestamp,
                    MP_price_in_stable: value,
                    Protocol: protocol.to_owned()
                };
                mp_assets.push(mp_asset);
            }
            None => {
                error!("Lpn currency not found in protocol {}", &protocol);
            }
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
