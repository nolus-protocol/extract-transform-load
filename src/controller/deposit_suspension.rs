use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::Error;

#[utoipa::path(
    get,
    path = "/api/deposit-suspension",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the deposit suspension threshold percentage.", body = Response)
    )
)]
#[get("/deposit-suspension")]
async fn index() -> Result<impl Responder, Error> {
    Ok(web::Json(Response {
        deposit_suspension: String::from("65.00"),
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Deposit suspension threshold percentage
    pub deposit_suspension: String,
}
