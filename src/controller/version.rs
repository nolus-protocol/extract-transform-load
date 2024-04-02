use crate::error::Error;
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/version")]
async fn index() -> Result<impl Responder, Error> {
    const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

    Ok(web::Json(Response{version: VERSION}))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response<'a> {
    pub version: Option<&'a str>,
}
