use crate::error::Error;
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/deposit-suspension")]
async fn index() -> Result<impl Responder, Error> {
    Ok(web::Json(Response {
        deposit_suspension: String::from("65.00"),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub deposit_suspension: String,
}
