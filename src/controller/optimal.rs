use std::convert::Infallible;

use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

#[get("/optimal")]
async fn index() -> Result<impl Responder, Infallible> {
    const { Ok(web::Json(Response { optimal: "70.00" })) }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub optimal: &'static str,
}
