use crate::{
    configuration::{AppState, State},
    error::Error,
    types::Bucket_Type,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[utoipa::path(
    get,
    path = "/api/history-stats",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns trading statistics including total trades, win rate, and average PnL for a wallet.", body = Response)
    )
)]
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

    Ok(web::Json(Response {
        pnl,
        tx_volume,
        win_rate,
        bucket,
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Response {
    /// Total realized PnL
    pub pnl: f64,
    /// Total transaction volume
    pub tx_volume: f64,
    /// Win rate percentage
    pub win_rate: f64,
    /// Distribution buckets
    pub bucket: Vec<Bucket_Type>,
}
