use actix_web::{
    get,
    web::{Data, Json},
    Responder,
};

use crate::{configuration::State, error::Error};

#[get("/blocks")]
async fn index(state: Data<State>) -> Result<impl Responder, Error> {
    state
        .database
        .block
        .count()
        .await
        .map(Json)
        .map_err(From::from)
}
