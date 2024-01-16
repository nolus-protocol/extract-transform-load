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

pub async fn fetch_insert(app_state: AppState<State>) -> Result<(), Error> {
    let mut joins = Vec::new();
    let mut mp_assets = vec![];
    let mut currencies = vec![];
    let timestamp = Utc::now();

    for protocol in app_state.protocols.values() {
        joins.push(
            app_state
                .query_api
                .get_prices(protocol.contracts.oracle.to_owned(), None),
        );
    }

    for result in join_all(joins).await {
        match result {
            Ok(data) => {
                if let Some(item) = data {
                    for price in item.prices {
                        if !currencies.contains(&price.amount.ticker){
                            let value = BigDecimal::from_str(&price.amount_quote.amount)?
                                / BigDecimal::from_str(&price.amount.amount)?;
                            let mp_asset = MP_Asset {
                                MP_asset_symbol: price.amount.ticker.to_owned(),
                                MP_asset_timestamp: timestamp,
                                MP_price_in_stable: value,
                            };
                            mp_assets.push(mp_asset);
                            currencies.push(price.amount.ticker.to_owned());
                        }
                    }
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    for stable in &app_state.config.lpns {
        let value = BigDecimal::from_str("1")?;
        let mp_asset = MP_Asset {
            MP_asset_symbol: stable.to_owned(),
            MP_asset_timestamp: timestamp,
            MP_price_in_stable: value,
        };
        mp_assets.push(mp_asset);
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
            if let Err(error) = fetch_insert(app).await {
                error!("Task error {}", error);
            };
        }
    })
    .await?
}
