use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};

#[get("/leased-assets")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state.database.ls_opening.get_leased_assets().await?;
    Ok(web::Json(data))
}
