use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/blocks")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    state
        .database
        .block
        .count()
        .await
        .map(web::Json)
        .map_err(From::from)
}
