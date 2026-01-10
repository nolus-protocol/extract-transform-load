use crate::{
    configuration::{AppState, State},
    error::Error,
    types::Bucket_Type,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/history-stats")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let (pnl, tx_volume, win_rate, bucket) = tokio::try_join!(
        state
            .database
            .ls_loan_closing
            .get_realized_pnl(address.to_owned()),
        state.database.raw_message.get_tx_volume(address.to_owned()),
        state.database.raw_message.get_win_rate(address.to_owned()),
        state.database.raw_message.get_buckets(address.to_owned())
    )?;

    Ok(web::Json(ResponseData {
        pnl,
        tx_volume,
        win_rate,
        bucket,
    }))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResponseData {
    pub pnl: f64,
    pub tx_volume: f64,
    pub win_rate: f64,
    pub bucket: Vec<Bucket_Type>,
}
