use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use anyhow::Context;
use chrono::{Duration, Utc};
use serde::Deserialize;
use std::str::FromStr;

#[get("/prices")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let mut interval = data.interval;

    if interval > 100 {
        interval = 100;
    }

    let group = getIntervalGroup(interval);
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

#[derive(Debug, Deserialize)]
pub struct Query {
    interval: i64,
    protocol: String,
    key: String,
}

pub fn getIntervalGroup(interval: i64) -> i32 {
    if interval <= 7 {
        return 5;
    } else if interval > 7 && interval < 30 {
        return 10;
    }

    60
}
