use crate::{
    configuration::{AppState, State},
    error::Error
};
use actix_web::{get, web, Responder, Result};

#[get("/time-series")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let data = state
        .database
        .lp_pool_state
        .get_supplied_borrowed_series()
        .await?;
    Ok(web::Json(data))
}
