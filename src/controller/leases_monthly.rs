use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};

#[get("/leases-monthly")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.ls_opening.get_leases_monthly().await?;
    Ok(web::Json(data))
}
