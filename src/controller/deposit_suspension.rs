use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

use crate::error::Error;

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
