use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Amount,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

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

    Ok(web::Json(ResponseData { position, debt }))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseData {
    pub position: Vec<LS_Amount>,
    pub debt: Vec<LS_Amount>,
}
