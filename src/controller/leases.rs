use actix_web::{
    get,
    web::{Data, Json, Query},
    Responder,
};
use serde::Deserialize;

use crate::{configuration::State, custom_uint::UInt63, error::Error};

#[get("/leases")]
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
        .ls_opening
        .get_leases_by_address(
            &address,
            skip.unwrap_or(const { UInt63::from_unsigned(0).unwrap() }),
            limit.map_or(
                const { UInt63::from_unsigned(10).unwrap() },
                |limit| limit.min(const { UInt63::from_unsigned(10).unwrap() }),
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
    address: String,
}
