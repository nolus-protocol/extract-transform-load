use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[utoipa::path(
    get,
    path = "/api/unrealized-pnl-by-address",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the unrealized profit and loss for a specific wallet address.", body = Response)
    )
)]
#[get("/unrealized-pnl-by-address")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let items = state
        .database
        .ls_state
        .get_unrealized_pnl_by_address(data.address.to_owned())
        .await?;

    Ok(web::Json(items))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Unrealized PnL in USD
    #[schema(value_type = f64)]
    pub unrealized_pnl: BigDecimal,
}
