use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use serde::{Deserialize, Serialize};

#[get("/message")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let (block,) = state.database.block.get_first_block().await?;
    Ok(web::Json(TestMessag { block }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestMessag {
    pub block: i64,
}
