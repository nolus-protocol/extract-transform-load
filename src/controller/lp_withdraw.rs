use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/lp-withdraw",
    tag = "Record Lookup",
    params(Query),
    responses(
        (status = 200, description = "Returns LP withdrawal details for a specific transaction hash. Returns 404 if transaction not found.", body = LpWithdrawResponse),
        (status = 404, description = "Transaction not found")
    )
)]
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Transaction hash
    tx: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LpWithdrawResponse {
    /// Transaction hash
    pub tx: String,
    /// Withdrawal amount
    #[schema(value_type = f64)]
    pub amount: BigDecimal,
    /// Pool name
    pub pool: String,
}
