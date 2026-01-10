use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[utoipa::path(
    get,
    path = "/api/position-debt-value",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns the current position value and outstanding debt for a specific wallet.", body = Response)
    )
)]
#[get("/position-debt-value")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let address = data.address.to_lowercase().to_owned();

    let position_fn = state
        .database
        .ls_opening
        .get_position_value(address.to_owned());
    let debt_fn = state.database.ls_opening.get_debt_value(address.to_owned());

    let (position, debt) = tokio::try_join!(position_fn, debt_fn)?;

    // Convert LS_Amount to AmountResponse
    let position: Vec<AmountResponse> = position
        .into_iter()
        .map(|a| AmountResponse {
            amount: a.amount,
            time: a.time,
        })
        .collect();
    let debt: Vec<AmountResponse> = debt
        .into_iter()
        .map(|a| AmountResponse {
            amount: a.amount,
            time: a.time,
        })
        .collect();

    Ok(web::Json(Response { position, debt }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Position values
    pub position: Vec<AmountResponse>,
    /// Debt values
    pub debt: Vec<AmountResponse>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AmountResponse {
    /// Amount value
    #[schema(value_type = String)]
    pub amount: BigDecimal,
    /// Timestamp
    pub time: DateTime<Utc>,
}
