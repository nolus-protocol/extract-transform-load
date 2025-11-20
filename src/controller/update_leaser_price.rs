use actix_web::{get, web, Responder};
use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/update/update-leaser-price")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response { result: false }));
    };

    let data = state.grpc.get_lease_raw_state_by_block(
        String::from(
            "nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t",
        ),
        17975902,
    ).await?;

    return Ok(web::Json(Response { result: true }));
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    auth: Option<String>,
}

async fn proceed(state: AppState<State>) -> Result<(), Error> {
    Ok(())
}
