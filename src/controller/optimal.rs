use std::convert::Infallible;

use actix_web::{get, web::Json, Responder};
use serde::Serialize;

#[get("/optimal")]
async fn index() -> Result<impl Responder, Infallible> {
    const { Ok(Json(Response { optimal: "70.00" })) }
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub optimal: &'static str,
}
