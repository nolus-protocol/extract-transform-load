use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, custom_uint::UInt63, error::Error};

#[get("/buyback")]
async fn index(
    state: Data<State>,
    Query(Arguments { skip, limit }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    state
        .database
        .tr_profit
        .get_buyback(
            skip.unwrap_or(const { UInt63::MIN }),
            limit.map_or(
                const { UInt63::from_unsigned(32).unwrap() },
                |limit| {
                    limit.min(const { UInt63::from_unsigned(100).unwrap() })
                },
            ),
        )
        .await
        .map(Json)
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    skip: Option<UInt63>,
    limit: Option<UInt63>,
}
