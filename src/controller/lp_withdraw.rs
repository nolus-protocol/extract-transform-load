use actix_web::{get, web, HttpResponse};
use serde::Deserialize;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/lp-withdraw")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let tx = data.tx.to_owned();
    match state.database.lp_withdraw.get_by_tx(tx).await? {
        Some(data) => Ok(HttpResponse::Ok().json(data)),
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Transaction not found"
        }))),
    }
}

#[derive(Debug, Deserialize)]
pub struct Query {
    tx: String,
}
