use std::borrow::Cow;

use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, custom_uint::UInt63, error::Error};

#[get("/borrow-apr")]
async fn index(
    state: Data<State>,
    Query(Arguments {
        skip,
        limit,
        mut protocol,
    }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    protocol.make_ascii_uppercase();

    let protocol = state
        .protocols
        .get(&protocol)
        .ok_or_else(|| Error::ProtocolError(Cow::Owned(protocol)))?;

    state
        .database
        .ls_opening
        .get_borrow_apr(
            &protocol.contracts.lpp,
            skip.unwrap_or(UInt63::MIN),
            limit
                .map(|limit| {
                    limit.min(const { UInt63::from_unsigned(100).unwrap() })
                })
                .unwrap_or(const { UInt63::from_unsigned(32).unwrap() }),
        )
        .await
        .map(|data| {
            Json(data.into_iter().map(|item| item.APR).collect::<Vec<_>>())
        })
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Arguments {
    skip: Option<UInt63>,
    limit: Option<UInt63>,
    protocol: String,
}
