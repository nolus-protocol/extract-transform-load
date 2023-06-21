use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future::join_all;
use tokio::task::JoinHandle;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{MP_Asset_Mapping, MP_Asset_State},
    types::MarketData,
};
use std::str::FromStr;

pub async fn fetch_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let data = app_state
        .database
        .mp_asset_mapping
        .get_all()
        .await
        .unwrap_or(vec![]);
    let mut joins = Vec::new();

    for item in data {
        joins.push(parse_and_insert(app_state.clone(), item, timestsamp));
    }

    let result = join_all(joins).await;

    for item in result {
        if let Err(e) = item {
            return Err(e);
        }
    }

    Ok(())
}

async fn parse_and_insert(
    app_state: AppState<State>,
    item: MP_Asset_Mapping,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let to = timestsamp.timestamp_millis() / 1000;
    let interval: i64 = app_state.config.aggregation_interval.into();
    let from = to - interval * 60 * 60;
    let from_date = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(from, 0), Utc);

    let market_data = app_state
        .http
        .get_coingecko_market_data_range(item.MP_asset_symbol_coingecko.to_owned(), from, to)
        .await?;

    let mut volume: f64 = 0.0;
    let mut market_cap: f64 = 0.0;

    let prices = market_data.prices;
    let open_price: f64 = prices.first().unwrap_or(&MarketData(0, 0.0)).1;
    let close_price: f64 = prices.last().unwrap_or(&MarketData(0, 0.0)).1;
    let (min_value, max_value) = app_state
        .database
        .mp_asset
        .get_min_max_from_range(item.MP_asset_symbol.to_owned(), from_date, timestsamp)
        .await
        .unwrap_or(Some((BigDecimal::from(0), BigDecimal::from(0))))
        .unwrap_or((BigDecimal::from(0), BigDecimal::from(0)));

    for item in &market_data.market_caps {
        market_cap += item.1;
    }

    for item in &market_data.total_volumes {
        volume += item.1;
    }

    if market_data.market_caps.len() > 0 {
        let market_len = market_data.market_caps.len() as f64;
        market_cap = market_cap / market_len;
    }

    if market_data.total_volumes.len() > 0 {
        let volume_len = market_data.total_volumes.len() as f64;
        volume = volume / volume_len;
    }

    let mp_asset_state = MP_Asset_State {
        MP_asset_symbol: item.MP_asset_symbol.to_owned(),
        MP_timestamp: timestsamp,
        MP_price_open: BigDecimal::from_str(&open_price.to_string())?,
        MP_price_high: BigDecimal::from_str(&max_value.to_string())?,
        MP_price_low: BigDecimal::from_str(&min_value.to_string())?,
        MP_price_close: BigDecimal::from_str(&close_price.to_string())?,
        MP_volume: BigDecimal::from_str(&volume.to_string())?,
        MP_marketcap: BigDecimal::from_str(&market_cap.to_string())?,
    };

    app_state
        .database
        .mp_asset_state
        .insert(mp_asset_state)
        .await?;

    Ok(())
}

pub fn start_task(app_state: AppState<State>, timestsamp: DateTime<Utc>) -> JoinHandle<Result<(), Error>>  {
    tokio::spawn(async move {
        fetch_insert(app_state, timestsamp).await
    })
}
