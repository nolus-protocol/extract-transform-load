use crate::{error::Error, configuration::{AppState, State}};
use actix_web::{get, web, HttpResponse, Result};
use serde::{Deserialize, Serialize};

#[get("/message")]
async fn index(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().into())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestMessag {
    pub test: bool,
}
