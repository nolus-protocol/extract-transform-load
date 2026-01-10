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
    path = "/api/realized-pnl-data",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns detailed realized PnL breakdown per lease for a specific wallet.", body = Vec<RealizedPnlDataResponse>)
    )
)]
#[get("/realized-pnl-data")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let data = state
        .database
        .ls_opening
        .get_realized_pnl_data(address)
        .await?;

    Ok(web::Json(data))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RealizedPnlDataResponse {
    /// Lease contract ID
    pub lease: String,
    /// Profit/Loss in USD
    #[schema(value_type = f64)]
    pub pnl: BigDecimal,
    /// Asset symbol
    pub symbol: String,
}
