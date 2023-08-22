use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Leased_Asset,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/leased-assets")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.ls_opening.get_leased_assets().await?;
    Ok(web::Json(Response { result: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: Vec<Leased_Asset>,
}
