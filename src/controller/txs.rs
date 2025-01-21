use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, error::Error};

#[get("/txs")]
async fn index(
    state: Data<State>,
    Query(Arguments {
        skip,
        limit,
        mut address,
    }): Query<Arguments>,
) -> Result<impl Responder, Error> {
    address.make_ascii_lowercase();

    state
        .database
        .raw_message
        .get(
            &address,
            skip.unwrap_or(0),
            limit.map_or(10, |limit| limit.min(100)),
        )
        .await
        .map(Json)
        .map_err(From::from)
}

#[derive(Debug, Deserialize)]
pub struct Arguments {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
}
