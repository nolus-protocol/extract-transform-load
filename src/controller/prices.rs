use actix_web::{get, web, Responder};
use anyhow::Context as _;
use chrono::{Duration, Utc};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/prices")]
async fn index(
    state: web::Data<AppState<State>>,
    web::Query(data): web::Query<Query>,
) -> Result<impl Responder, Error> {
    let interval = data.interval.min(100);

    const BIG_DECIMAL_TO_FLOAT_ERROR: &str = "Couldn't convert BigDecimal to \
    floating-point number via string conversion!";

    state
        .database
        .mp_asset
        .get_prices(
            data.key,
            data.protocol,
            Utc::now() - Duration::days(interval),
            getIntervalGroup(interval),
        )
        .await
        .map_err(From::from)
        .and_then(|data| {
            data.into_iter()
                .try_fold(vec![], |mut prices, (date, price)| {
                    price
                        .to_string()
                        .parse::<f64>()
                        .context(BIG_DECIMAL_TO_FLOAT_ERROR)
                        .map(|price| {
                            prices.push((date, price));

                            prices
                        })
                })
                .map(web::Json)
                .map_err(From::from)
        })
}

#[derive(Debug, Deserialize)]
pub struct Query {
    interval: i64,
    protocol: String,
    key: String,
}

pub fn getIntervalGroup(interval: i64) -> i32 {
    if interval <= 7 {
        1
    } else if interval > 7 && interval < 30 {
        5
    } else {
        60
    }
}
