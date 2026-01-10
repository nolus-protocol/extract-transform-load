use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::Error;

#[utoipa::path(
    get,
    path = "/api/version",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the current version of the ETL service.", body = Response)
    )
)]
#[get("/version")]
async fn index() -> Result<impl Responder, Error> {
    const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

    Ok(web::Json(Response {
        version: VERSION.map(|v| v.to_string()),
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// ETL service version
    pub version: Option<String>,
}
