use actix_web::{get, web, Responder};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/lp-withdraw")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let tx = data.tx.to_owned();
    let data = state.database.lp_withdraw.get_by_tx(tx).await?;
    Ok(web::Json(data))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    tx: String,
}
