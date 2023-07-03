use chrono::Utc;
use futures::future::join_all;
use sqlx::types::BigDecimal;
use tokio::{task::JoinHandle, time, time::Duration};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Actions,
    model::{Action_History, MP_Asset},
};
use std::str::FromStr;
use super::mp_map_assets;

pub async fn fetch_insert(app_state: AppState<State>) -> Result<(), Error> {
    let (data, ids) = mp_map_assets::get_mappings(&app_state.database.mp_asset_mapping).await;
    let prices = app_state.http.get_coingecko_prices(&ids).await?;
    let mut joins = Vec::new();
    let timestamp = Utc::now();

    for (key, value) in prices {
        let item = data
            .iter()
            .position(|item| item.MP_asset_symbol_coingecko == key)
            .ok_or(Error::FieldNotExist(String::from("MP_ASSET")))?;

        let item = &data[item];
        let stable_currency = app_state.config.stable_currency.to_lowercase();
        let value = value
            .get(&stable_currency)
            .ok_or(Error::FieldNotExist(String::from("currency")))?;
        let value = BigDecimal::from_str(&value.to_string())?;

        let mp_asset = MP_Asset {
            MP_asset_symbol: item.MP_asset_symbol.to_owned(),
            MP_asset_timestamp: timestamp,
            MP_price_in_stable: value,
        };

        joins.push(app_state.database.mp_asset.insert(mp_asset));
    }

    let action_history = Action_History {
        action_type: Actions::MpAssetAction.to_string(),
        created_at: timestamp,
    };

    app_state
        .database
        .action_history
        .insert(action_history)
        .await?;

    let result = join_all(joins).await;

    for item in result {
        if let Err(e) = item {
            return Err(Error::SQL(e));
        }
    }

    Ok(())
}

pub fn mp_assets_task(app_state: AppState<State>) -> JoinHandle<Result<(), Error>> {
    let interval: u64 = app_state.config.mp_asset_interval.into();
    let interval: u64 = interval * 60;

    let mut interval = time::interval(Duration::from_secs(interval));
    tokio::spawn(async move {
        interval.tick().await;
        loop {
            interval.tick().await;
            let app = app_state.clone();
            if let Err(error) = fetch_insert(app).await {
                return Err(Error::TaskError(error.to_string()));
            };
        }
    })
}
