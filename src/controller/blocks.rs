use actix_web::{get, web, Responder};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[utoipa::path(
    get,
    path = "/api/blocks",
    tag = "Protocol Analytics",
    responses(
        (status = 200, description = "Returns the number of blockchain blocks synced by the ETL service.", body = Response)
    )
)]
#[get("/blocks")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.block.count().await?;
    Ok(web::Json(Response { blocks: data }))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response {
    /// Number of synced blocks
    pub blocks: i64,
}
