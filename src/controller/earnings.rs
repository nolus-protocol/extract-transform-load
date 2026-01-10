use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/earnings",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the total earnings from lending for a specific wallet address.", body = Response)
    )
)]
#[get("/earnings")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();
    let earnings = state.database.lp_pool_state.get_earnings(address).await?;
    Ok(web::Json(Response { earnings }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Total earnings in USD
    #[schema(value_type = f64)]
    pub earnings: BigDecimal,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}
