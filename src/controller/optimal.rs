use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[get("/optimal")]
async fn index() -> Result<impl Responder, Error> {
    Ok(web::Json(Response {
        optimal: String::from("70.00"),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub optimal: String,
}
