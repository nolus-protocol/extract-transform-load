use std::convert::Infallible;

use actix_web::{get, web::Json, Responder};
use serde::Serialize;

#[get("/deposit-suspension")]
async fn index() -> Result<impl Responder, Infallible> {
    const {
        Ok(Json(Response {
            deposit_suspension: "65.00",
        }))
    }
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub deposit_suspension: &'static str,
}
