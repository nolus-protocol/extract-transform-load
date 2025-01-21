use std::collections::BTreeMap;

use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use bigdecimal::ToPrimitive;
use chrono::{Duration, Utc};
use serde::Deserialize;

use crate::{
    configuration::State,
    custom_uint::{UInt63, UInt7},
    error::Error,
};

#[get("/prices")]
async fn index(
    state: Data<State>,
    Query(Arguments {
        interval,
        protocol,
        key,
    }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    let interval = interval.min(const { UInt63::from_unsigned(100).unwrap() });

    state
        .database
        .mp_asset
        .get_prices(
            &key,
            &protocol,
            Utc::now() - Duration::days(interval.get_signed()),
            getIntervalGroup(interval.get()),
        )
        .await
        .map(|prices| {
            Json(
                prices
                    .into_iter()
                    .map(|(date, price)| {
                        (date.timestamp_millis(), price.to_f64())
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    interval: UInt63,
    protocol: String,
    key: String,
}

pub fn getIntervalGroup(interval: u64) -> UInt7 {
    if interval <= 7 {
        const { UInt7::from_unsigned(1).unwrap() }
    } else if interval > 7 && interval < 30 {
        const { UInt7::from_unsigned(5).unwrap() }
    } else {
        const { UInt7::from_unsigned(60).unwrap() }
    }
}
