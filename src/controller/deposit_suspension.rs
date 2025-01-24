use std::convert::Infallible;

use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};

#[get("/deposit-suspension")]
async fn index() -> Result<impl Responder, Infallible> {
    const {
        Ok(web::Json(Response {
            deposit_suspension: "65.00",
        }))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub deposit_suspension: &'static str,
}
