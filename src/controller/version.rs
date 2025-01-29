use std::convert::Infallible;

use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

#[get("/version")]
async fn index() -> Result<impl Responder, Infallible> {
    const {
        Ok(web::Json(Response {
            version: env!("CARGO_PKG_VERSION"),
        }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub version: &'static str,
}
