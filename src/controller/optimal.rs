use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::Error;

#[utoipa::path(
    get,
    path = "/api/optimal",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the optimal utilization rate threshold configured for the protocol.", body = Response)
    )
)]
#[get("/optimal")]
async fn index() -> Result<impl Responder, Error> {
    Ok(web::Json(Response {
        optimal: String::from("70.00"),
    }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Optimal utilization rate percentage
    pub optimal: String,
}
