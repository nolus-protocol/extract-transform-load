use std::str::FromStr as _;

use actix_web::{get, web, Responder};
use anyhow::Context as _;
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/prices",
    tag = "Market Data",
    params(Query),
    responses(
        (status = 200, description = "Returns historical price data for assets with interval and protocol filtering.", body = Vec<PricePoint>)
    )
)]
#[get("/prices")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let mut interval = data.interval;

    if interval > 100 {
        interval = 100;
    }

    let group = get_interval_group(interval);
    let date = Utc::now() - Duration::days(interval);

    let data = state
        .database
        .mp_asset
        .get_prices(data.key.to_owned(), data.protocol.to_owned(), date, group)
        .await?;
    let mut prices = vec![];

    for (date, price) in data.into_iter() {
        let ms = date.timestamp_millis();
        let str_price = price.to_string();
        let p = f64::from_str(&str_price)
            .context("coudld not parse big decimal to float")?;
        prices.push((ms, p));
    }

    Ok(web::Json(prices))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Time interval in days (max 100)
    interval: i64,
    /// Protocol identifier (e.g., OSMOSIS-OSMOSIS-USDC_NOBLE)
    protocol: String,
    /// Asset symbol (e.g., ATOM, OSMO)
    key: String,
}

pub fn get_interval_group(interval: i64) -> i32 {
    if interval <= 7 {
        return 1;
    } else if interval > 7 && interval < 30 {
        return 5;
    }

    60
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PricePoint(
    /// Timestamp in milliseconds
    pub i64,
    /// Price value
    pub f64,
);
